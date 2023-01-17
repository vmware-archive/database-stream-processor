use crate::{
    codegen::{
        intrinsics::ImportIntrinsics, utils::FunctionBuilderExt, vtable::column_non_null, Codegen,
        CodegenConfig, Layout, TRAP_NULL_PTR,
    },
    ir::{LayoutId, RowLayout, RowType},
};
use cranelift::prelude::{FunctionBuilder, InstBuilder, IntCC, MemFlags, TrapCode, Value};
use cranelift_jit::JITModule;
use cranelift_module::{FuncId, Module};

// FIXME: For non-trivial layouts we could potentially encounter leaks if
// cloning panics part of the way through. For example, if while cloning a `{
// string, string }` we clone the first string successfully and then panic while
// cloning the second string (due to a failed allocation, for example), the
// first successfully cloned string would be leaked. The same effect happens
// with `clone_into_slice`, except with all successfully cloned elements instead
// of just with successfully cloned fields. We probably want to fix that
// sometime by integrating panic handling into our clone routines even though
// this is a fairly minimal consequence of an edge case.

impl Codegen {
    /// Generates a function cloning the given layout
    // FIXME: This also ignores the existence of strings
    pub fn codegen_layout_clone(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *mut u8)
        let func_id = self.new_function([self.module.isa().pointer_type(); 2], None);
        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_entry_block();
            let params = builder.block_params(entry_block);
            let (src, dest) = (params[0], params[1]);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            // Zero sized types have nothing to clone
            if !layout.is_zero_sized() {
                // If debug assertions are enabled, trap if `src` or `dest` are null or if
                // `src == dest`
                if self.config.debug_assertions {
                    builder.ins().trapz(src, TRAP_NULL_PTR);
                    builder.ins().trapz(dest, TRAP_NULL_PTR);

                    let src_eq_dest = builder.ins().icmp(IntCC::Equal, src, dest);
                    builder
                        .ins()
                        .trapnz(src_eq_dest, TrapCode::UnreachableCodeReached);
                }

                // If the row contains types that require non-trivial cloning (e.g. strings)
                // we have to manually clone each field
                if row_layout.requires_nontrivial_clone() {
                    clone_layout(
                        src,
                        dest,
                        layout,
                        &row_layout,
                        &mut builder,
                        &mut imports,
                        &mut self.module,
                        &self.config,
                    );

                // If the row is just scalar types we can simply memcpy it
                } else {
                    let align = layout.align().try_into().unwrap();

                    // TODO: We can make our own more efficient memcpy here, the one that ships with
                    // cranelift is eh
                    builder.emit_small_memory_copy(
                        self.module.isa().frontend_config(),
                        src,
                        dest,
                        layout.size() as u64,
                        align,
                        align,
                        true,
                        MemFlags::trusted(),
                    );
                }
            }

            builder.ins().return_(&[]);

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }

    /// Generates a function cloning a slice of the given layout
    // FIXME: This also ignores the existence of strings
    pub fn codegen_layout_clone_into_slice(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *mut u8, usize)
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_function([ptr_ty; 3], None);
        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            let entry_block = builder.create_entry_block();
            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            // Zero sized types have nothing to clone
            if !layout.is_zero_sized() {
                let params = builder.block_params(entry_block);
                let (src, dest, length) = (params[0], params[1], params[2]);

                // If debug assertions are enabled, trap if `src` or `dest` are null or if
                // `src == dest`
                if self.config.debug_assertions {
                    builder.ins().trapz(src, TRAP_NULL_PTR);
                    builder.ins().trapz(dest, TRAP_NULL_PTR);

                    let src_eq_dest = builder.ins().icmp(IntCC::Equal, src, dest);
                    builder
                        .ins()
                        .trapnz(src_eq_dest, TrapCode::UnreachableCodeReached);
                }

                // For non-trivial layouts we have to manually clone things
                if row_layout.requires_nontrivial_clone() {
                    // TODO: I wonder if it wouldn't be more efficient to memcpy the entire source
                    // slice into the destination slice and then iterate over
                    // the destination slice while cloning strings in-place, e.g.
                    //
                    // ```
                    // // layout is a `{ string, u32 }`
                    // memcpy(src, dest, sizeof(layout) * length);
                    //
                    // let mut current = dest;
                    // let end = dest.add(length);
                    // while current < end {
                    //     let place = current.add(offsetof(layout.0));
                    //
                    //     let current_val = place.read();
                    //     let cloned = clone_string(current_val);
                    //     place.write(cloned);
                    //
                    //     current = current.add(sizeof(layout));
                    // }
                    // ```

                    // Build a tail-controlled loop to clone all elements
                    // ```
                    // entry(src, dest, length):
                    //   bytes = imul length, sizeof(layout)
                    //   src_end = iadd src, bytes
                    //
                    //   // Check if `length` is zero and if so, skip cloning
                    //   brz length, tail
                    //   jump body(src, dest)
                    //
                    // body(src, dest):
                    //   // clone columns...
                    //
                    //   src_inc = iadd src, sizeof(layout)
                    //   dest_inc = iadd dest, sizeof(layout)
                    //   inbounds = icmp ult src_inc, src_end
                    //   brnz inbounds, body(src_inc, dest_inc)
                    //   jump tail
                    //
                    // tail:
                    //   return
                    // ```

                    let tail = builder.create_block();
                    let body = builder.create_block();
                    // TODO: Is there a meaningful difference between phi-ing over an offset vs.
                    // phi-ing over the two incremented pointers?
                    builder.append_block_param(body, ptr_ty);
                    builder.append_block_param(body, ptr_ty);

                    // Calculate the slice's end pointer
                    let length_bytes = builder.ins().imul_imm(length, layout.size() as i64);
                    let src_end = builder.ins().iadd(src, length_bytes);

                    // Check that `length` isn't zero and if so jump to the end
                    builder.ins().brz(length, tail, &[]);
                    builder.ins().jump(body, &[src, dest]);

                    builder.seal_block(entry_block);
                    builder.switch_to_block(body);

                    let params = builder.block_params(body);
                    let (src, dest) = (params[0], params[1]);
                    clone_layout(
                        src,
                        dest,
                        layout,
                        &row_layout,
                        &mut builder,
                        &mut imports,
                        &mut self.module,
                        &self.config,
                    );

                    // Increment both pointers
                    let src_inc = builder.ins().iadd_imm(src, layout.size() as i64);
                    let dest_inc = builder.ins().iadd_imm(dest, layout.size() as i64);

                    // Check if we should continue iterating
                    let ptr_inbounds =
                        builder
                            .ins()
                            .icmp(IntCC::UnsignedLessThan, src_inc, src_end);
                    builder.ins().brnz(ptr_inbounds, body, &[src_inc, dest_inc]);
                    builder.ins().jump(tail, &[]);

                    builder.seal_current();
                    builder.switch_to_block(tail);

                // For types consisting entirely of scalar values we can simply
                // emit a memcpy
                } else {
                    // The total size we need to copy is size_of(layout) * length
                    // TODO: Should we add a size/overflow assertion here? Just for
                    // `debug_assertions`?
                    let size = builder.ins().imul_imm(length, layout.size() as i64);
                    builder.call_memcpy(self.module.isa().frontend_config(), src, dest, size);
                }
            }

            builder.ins().return_(&[]);

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }
}

// TODO: We can copy over the bitflag bytes wholesale without doing the whole
// "check bit, set bit, write bit" thing
fn clone_layout(
    src: Value,
    dest: Value,
    layout: &Layout,
    row_layout: &RowLayout,
    builder: &mut FunctionBuilder,
    imports: &mut ImportIntrinsics,
    module: &mut JITModule,
    config: &CodegenConfig,
) {
    debug_assert!(row_layout.requires_nontrivial_clone());

    let src_flags = MemFlags::trusted().with_readonly();
    let dest_flags = MemFlags::trusted();

    // TODO: We should do this in layout order instead of field order so we can
    // potentially fuse loads/stores. Even better would be to clone in layout order
    // with padding bytes interspersed (also in layout order) for maximal
    // optimization potential
    for (idx, (ty, nullable)) in row_layout.iter().enumerate() {
        if ty.is_unit() && !nullable {
            continue;
        }

        // TODO: For nullable scalar values we can unconditionally copy them over, we
        // only need to branch for non-trivial clones
        let next_clone = if nullable {
            // Zero = value isn't null, non-zero = value is null
            let value_non_null = column_non_null(idx, src, layout, builder, config, module, true);

            // If the value is null, set the cloned value to null
            let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(idx);
            let bitset_ty = bitset_ty.native_type(&module.isa().frontend_config());
            let mask = 1 << bit_idx;

            // Load the bitset's current value
            let current_bitset =
                builder
                    .ins()
                    .load(bitset_ty, dest_flags, dest, bitset_offset as i32);

            debug_assert!(config.null_sigil.is_one());
            let bitset_with_null = builder.ins().bor_imm(current_bitset, mask);
            let bitset_with_non_null = builder.ins().band_imm(current_bitset, !mask);
            let bitset =
                builder
                    .ins()
                    .select(value_non_null, bitset_with_null, bitset_with_non_null);

            // Store the newly modified bitset back into the row
            builder
                .ins()
                .store(dest_flags, bitset, dest, bitset_offset as i32);

            // For nullable unit types we don't need to do anything else
            if ty.is_unit() {
                continue;

            // For everything else we have to actually clone their inner value
            } else {
                let clone_innards = builder.create_block();
                let next_clone = builder.create_block();
                builder.ins().brnz(value_non_null, next_clone, &[]);
                builder.ins().jump(clone_innards, &[]);

                builder.switch_to_block(clone_innards);
                Some(next_clone)
            }
        } else {
            None
        };

        debug_assert!(!ty.is_unit());

        let offset = layout.offset_of(idx) as i32;
        let native_ty = layout
            .type_of(idx)
            .native_type(&module.isa().frontend_config());

        // Load the source value
        let src_value = builder.ins().load(native_ty, src_flags, src, offset);

        // Clone the source value
        let cloned = match ty {
            // For scalar types we just copy the value directly
            RowType::Bool
            | RowType::U16
            | RowType::U32
            | RowType::U64
            | RowType::I16
            | RowType::I32
            | RowType::I64
            | RowType::F32
            | RowType::F64 => src_value,

            // Strings need their clone function called
            RowType::String => {
                let clone_string = imports.string_clone(module, builder.func);
                builder.call_fn(clone_string, &[src_value])
            }

            // Unit types have been handled
            RowType::Unit => unreachable!(),
        };

        // Store the cloned value
        builder.ins().store(dest_flags, cloned, dest, offset);

        if let Some(next_clone) = next_clone {
            builder.ins().jump(next_clone, &[]);
            builder.switch_to_block(next_clone);
        }
    }
}
