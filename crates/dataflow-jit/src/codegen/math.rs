use crate::codegen::{utils::FunctionBuilderExt, CodegenCtx, TRAP_DIV_OVERFLOW};
use cranelift::prelude::{types, FloatCC, FunctionBuilder, InstBuilder, IntCC, MemFlags, Value};

impl CodegenCtx<'_> {
    /// Check if a float is NaN
    pub(super) fn float_is_nan(&mut self, x: Value, builder: &mut FunctionBuilder<'_>) -> Value {
        debug_assert!(builder.value_type(x).is_float());
        // TODO: Float comparison conditions are confusing, I think this is correct
        // though
        builder.ins().fcmp(FloatCC::Unordered, x, x)
    }

    /// Based off of rust's [`f32::total_cmp()`] and [`f64::total_cmp()`]
    /// implementations
    ///
    /// ```rust,ignore
    /// // f32::total_cmp()
    /// pub fn total_cmp(&self, other: &Self) -> Ordering {
    ///     let mut left = self.to_bits() as i32;
    ///     let mut right = other.to_bits() as i32;
    ///
    ///     // In case of negatives, flip all the bits except the sign
    ///     // to achieve a similar layout as two's complement integers
    ///     //
    ///     // Why does this work? IEEE 754 floats consist of three fields:
    ///     // Sign bit, exponent and mantissa. The set of exponent and mantissa
    ///     // fields as a whole have the property that their bitwise order is
    ///     // equal to the numeric magnitude where the magnitude is defined.
    ///     // The magnitude is not normally defined on NaN values, but
    ///     // IEEE 754 totalOrder defines the NaN values also to follow the
    ///     // bitwise order. This leads to order explained in the doc comment.
    ///     // However, the representation of magnitude is the same for negative
    ///     // and positive numbers – only the sign bit is different.
    ///     // To easily compare the floats as signed integers, we need to
    ///     // flip the exponent and mantissa bits in case of negative numbers.
    ///     // We effectively convert the numbers to "two's complement" form.
    ///     //
    ///     // To do the flipping, we construct a mask and XOR against it.
    ///     // We branchlessly calculate an "all-ones except for the sign bit"
    ///     // mask from negative-signed values: right shifting sign-extends
    ///     // the integer, so we "fill" the mask with sign bits, and then
    ///     // convert to unsigned to push one more zero bit.
    ///     // On positive values, the mask is all zeros, so it's a no-op.
    ///     left ^= (((left >> 31) as u32) >> 1) as i32;
    ///     right ^= (((right >> 31) as u32) >> 1) as i32;
    ///
    ///     left.cmp(&right)
    /// }
    /// ```
    ///
    /// [`f32::total_cmp()`]: https://doc.rust-lang.org/std/primitive.f32.html#method.total_cmp
    /// [`f64::total_cmp()`]: https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp
    pub(super) fn normalize_float(&self, float: Value, builder: &mut FunctionBuilder<'_>) -> Value {
        let ty = builder.value_type(float);
        let (int_ty, first_shift) = if ty == types::F32 {
            (types::I32, 31)
        } else if ty == types::F64 {
            (types::I64, 63)
        } else {
            unreachable!("normalize_float() can only be called on f32 and f64: {ty}")
        };

        // float.to_bits()
        // TODO: Should we apply any flags to this?
        let int = builder.ins().bitcast(int_ty, MemFlags::new(), float);

        if let Some(writer) = self.comment_writer.as_deref() {
            writer.borrow_mut().add_comment(
                builder.value_def(int),
                format!("normalize {ty} for totalOrder"),
            );
        }

        // left >> {31, 63}
        let shifted = builder.ins().sshr_imm(int, first_shift);
        // ((left >> {31, 63}) as {u32, u64}) >> 1
        let shifted = builder.ins().ushr_imm(shifted, 1);

        // left ^= shifted
        builder.ins().bxor(int, shifted)
    }

    pub(super) fn float_lt(
        &self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        if self.config.total_float_comparisons {
            let (lhs, rhs) = (
                self.normalize_float(lhs, builder),
                self.normalize_float(rhs, builder),
            );
            builder.ins().icmp(IntCC::SignedLessThan, lhs, rhs)
        } else {
            builder.ins().fcmp(FloatCC::LessThan, lhs, rhs)
        }
    }

    pub(super) fn float_gt(
        &self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        if self.config.total_float_comparisons {
            let (lhs, rhs) = (
                self.normalize_float(lhs, builder),
                self.normalize_float(rhs, builder),
            );
            builder.ins().icmp(IntCC::SignedGreaterThan, lhs, rhs)
        } else {
            builder.ins().fcmp(FloatCC::GreaterThan, lhs, rhs)
        }
    }

    /// Simultaneous floored integer division and modulus
    ///
    /// ```rust,ignore
    /// fn div_mod_floor(&self, other: &Self) -> (Self, Self) {
    ///     (self.div_floor(other), self.mod_floor(other))
    /// }
    ///
    /// fn div_floor(&self, other: &Self) -> Self {
    ///     // Algorithm from [Daan Leijen. _Division and Modulus for Computer Scientists_,
    ///     // December 2001](http://research.microsoft.com/pubs/151917/divmodnote-letter.pdf)
    ///     let (d, r) = self.div_rem(other);
    ///     if (r > 0 && *other < 0) || (r < 0 && *other > 0) {
    ///         d - 1
    ///     } else {
    ///         d
    ///     }
    /// }
    ///
    /// fn mod_floor(&self, other: &Self) -> Self {
    ///     // Algorithm from [Daan Leijen. _Division and Modulus for Computer Scientists_,
    ///     // December 2001](http://research.microsoft.com/pubs/151917/divmodnote-letter.pdf)
    ///     let r = *self % *other;
    ///     if (r > 0 && *other < 0) || (r < 0 && *other > 0) {
    ///         r + *other
    ///     } else {
    ///         r
    ///     }
    /// }
    /// ```
    #[allow(dead_code)]
    pub(super) fn sdiv_mod_floor(
        &self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> (Value, Value) {
        let lhs_ty = builder.value_type(lhs);
        debug_assert!(lhs_ty.is_int());
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));

        let (div, rem) = self.div_rem(true, lhs, rhs, builder);

        // if let Some(writer) = self.comment_writer.as_deref() {
        //     writer.borrow_mut().add_comment(
        //         builder.func.dfg.value_def(div).unwrap_inst(),
        //         format!("div_mod_floor({lhs}, {rhs})"),
        //     );
        // }

        let zero = builder.ins().iconst(lhs_ty, 0);
        let rem_gt_zero = builder.ins().icmp(IntCC::SignedGreaterThan, rem, zero);
        let rhs_lt_zero = builder.ins().icmp(IntCC::SignedLessThan, rhs, zero);
        let rem_lt_zero = builder.ins().icmp(IntCC::SignedLessThan, rem, zero);
        let rhs_gt_zero = builder.ins().icmp(IntCC::SignedGreaterThan, rhs, zero);

        let gt_lt = builder.ins().band(rem_gt_zero, rhs_lt_zero);
        let lt_gt = builder.ins().band(rem_lt_zero, rhs_gt_zero);
        let use_floored = builder.ins().bor(gt_lt, lt_gt);

        let one = builder.ins().iconst(lhs_ty, 1);
        let div_sub_one = builder.ins().isub(div, one);
        let rem_plus_rhs = builder.ins().iadd(rem, rhs);

        let div_floor = builder.ins().select(use_floored, div_sub_one, div);
        let mod_floor = builder.ins().select(use_floored, rem_plus_rhs, rem);

        (div_floor, mod_floor)
    }

    /// Floored integer division
    pub(super) fn div_floor(
        &self,
        signed: bool,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let lhs_ty = builder.value_type(lhs);
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));
        debug_assert!(lhs_ty.is_int());

        // Signed floored division
        //
        // ```rust,ignore
        // fn div_floor(&self, other: &Self) -> Self {
        //     // Algorithm from [Daan Leijen. _Division and Modulus for Computer Scientists_,
        //     // December 2001](http://research.microsoft.com/pubs/151917/divmodnote-letter.pdf)
        //     let (d, r) = self.div_rem(other);
        //     if (r > 0 && *other < 0) || (r < 0 && *other > 0) {
        //         d - 1
        //     } else {
        //         d
        //     }
        // }
        // ```
        if signed {
            let (div, rem) = self.div_rem(true, lhs, rhs, builder);

            let zero = builder.ins().iconst(lhs_ty, 0);
            let rem_gt_zero = builder.ins().icmp(IntCC::SignedGreaterThan, rem, zero);
            let rhs_lt_zero = builder.ins().icmp(IntCC::SignedLessThan, rhs, zero);
            let rem_lt_zero = builder.ins().icmp(IntCC::SignedLessThan, rem, zero);
            let rhs_gt_zero = builder.ins().icmp(IntCC::SignedGreaterThan, rhs, zero);

            let gt_lt = builder.ins().band(rem_gt_zero, rhs_lt_zero);
            let lt_gt = builder.ins().band(rem_lt_zero, rhs_gt_zero);
            let use_div_sub_one = builder.ins().bor(gt_lt, lt_gt);

            let one = builder.ins().iconst(lhs_ty, 1);
            let div_sub_one = builder.ins().isub(div, one);

            builder.ins().select(use_div_sub_one, div_sub_one, div)

        // Unsigned div_floor is identical to an unsigned division
        } else {
            builder.ins().udiv(lhs, rhs)
        }
    }

    /// Floored integer modulo
    pub(super) fn mod_floor(
        &self,
        signed: bool,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let lhs_ty = builder.value_type(lhs);
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));
        debug_assert!(lhs_ty.is_int());

        // Signed floored modulus
        //
        // ```rust,ignore
        // fn mod_floor(&self, other: &Self) -> Self {
        //     // Algorithm from [Daan Leijen. _Division and Modulus for Computer Scientists_,
        //     // December 2001](http://research.microsoft.com/pubs/151917/divmodnote-letter.pdf)
        //     let r = *self % *other;
        //     if (r > 0 && *other < 0) || (r < 0 && *other > 0) {
        //         r + *other
        //     } else {
        //         r
        //     }
        // }
        // ```
        if signed {
            let rem = builder.ins().srem(lhs, rhs);

            let zero = builder.ins().iconst(lhs_ty, 0);
            let rem_gt_zero = builder.ins().icmp(IntCC::SignedGreaterThan, rem, zero);
            let rhs_lt_zero = builder.ins().icmp(IntCC::SignedLessThan, rhs, zero);
            let rem_lt_zero = builder.ins().icmp(IntCC::SignedLessThan, rem, zero);
            let rhs_gt_zero = builder.ins().icmp(IntCC::SignedGreaterThan, rhs, zero);

            let gt_lt = builder.ins().band(rem_gt_zero, rhs_lt_zero);
            let lt_gt = builder.ins().band(rem_lt_zero, rhs_gt_zero);
            let use_rem_plus_rhs = builder.ins().bor(gt_lt, lt_gt);

            let rem_plus_rhs = builder.ins().iadd(rem, rhs);

            builder.ins().select(use_rem_plus_rhs, rem_plus_rhs, rem)

        // Unsigned floored modulus is identical to an unsigned remainder
        } else {
            builder.ins().urem(lhs, rhs)
        }
    }

    /// Simultaneous truncated integer division and modulus.
    ///
    /// ```rust,ignore
    /// fn div_rem(&self, other: &Self) -> (Self, Self) {
    ///     (*self / *other, *self % *other)
    /// }
    /// ```
    pub(super) fn div_rem(
        &self,
        signed: bool,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> (Value, Value) {
        if signed {
            let div = self.sdiv_checked(lhs, rhs, builder);
            let rem = builder.ins().srem(lhs, rhs);
            (div, rem)
        } else {
            let div = builder.ins().udiv(lhs, rhs);
            let rem = builder.ins().urem(lhs, rhs);
            (div, rem)
        }
    }

    #[allow(dead_code)]
    pub(super) fn sdiv_euclid(
        &self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let lhs_ty = builder.value_type(lhs);
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));
        debug_assert!(lhs_ty.is_int());

        // ```rust,ignore
        // fn div_euclid(self, rhs: Self) -> Self {
        //     let q = self / rhs;
        //     if self % rhs < 0 {
        //         return if rhs > 0 { q - 1 } else { q + 1 };
        //     }
        //     q
        // }
        // ```
        let zero = builder.ins().iconst(lhs_ty, 0);

        let q = self.sdiv_checked(lhs, rhs, builder);
        // self.comment(builder.value_inst(q), || {
        //     format!("sdiv_euclid({lhs}, {rhs})")
        // });

        let lt_zero = builder.create_block();
        let after = builder.create_block();
        builder.append_block_param(after, lhs_ty);

        let modulus = builder.ins().srem(lhs, rhs);
        let mod_lt_zero = builder.ins().icmp(IntCC::SignedLessThan, modulus, zero);

        builder.ins().brif(mod_lt_zero, lt_zero, &[], after, &[q]);
        builder.seal_current();
        builder.switch_to_block(lt_zero);

        let rhs_gt_zero = builder.ins().icmp(IntCC::SignedGreaterThan, rhs, zero);
        let one = builder.ins().iconst(lhs_ty, 1);
        let q_sub_one = builder.ins().isub(q, one);
        let q_plus_one = builder.ins().iadd(q, one);
        let div = builder.ins().select(rhs_gt_zero, q_sub_one, q_plus_one);
        builder.ins().jump(after, &[div]);

        builder.seal_current();
        builder.switch_to_block(after);
        builder.block_params(after)[0]
    }

    #[allow(dead_code)]
    pub(super) fn fdiv_euclid(
        &mut self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let lhs_ty = builder.value_type(lhs);
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));
        debug_assert!(lhs_ty.is_float());

        // ```rust,ignore
        // fn div_euclid(self, rhs: f32) -> f32 {
        //     let q = (self / rhs).trunc();
        //     if self % rhs < 0.0 {
        //         return if rhs > 0.0 { q - 1.0 } else { q + 1.0 };
        //     }
        //     q
        // }
        // ```
        let zero = if lhs_ty == types::F32 {
            builder.ins().f32const(0.0)
        } else {
            builder.ins().f64const(0.0)
        };

        let div = builder.ins().fdiv(lhs, rhs);
        let q = builder.ins().trunc(div);
        self.comment(builder.value_def(div), || {
            format!("fdiv_euclid({lhs}, {rhs})")
        });

        let lt_zero = builder.create_block();
        let after = builder.create_block();
        builder.append_block_param(after, lhs_ty);

        let modulus = self.fmod(lhs, rhs, builder);
        // TODO: Should this use total float comparisons?
        let mod_lt_zero = self.float_lt(modulus, zero, builder);

        builder.ins().brif(mod_lt_zero, lt_zero, &[], after, &[q]);
        builder.seal_current();
        builder.switch_to_block(lt_zero);

        // TODO: Should this use total float comparisons?
        let rhs_gt_zero = self.float_gt(rhs, zero, builder);
        let one = if lhs_ty == types::F32 {
            builder.ins().f32const(1.0)
        } else {
            builder.ins().f64const(1.0)
        };
        let q_sub_one = builder.ins().fsub(q, one);
        let q_plus_one = builder.ins().fadd(q, one);
        let div = builder.ins().select(rhs_gt_zero, q_sub_one, q_plus_one);
        builder.ins().jump(after, &[div]);

        builder.seal_current();
        builder.switch_to_block(after);
        builder.block_params(after)[0]
    }

    pub(super) fn srem_euclid(
        &self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let lhs_ty = builder.value_type(lhs);
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));
        debug_assert!(lhs_ty.is_int());

        // ```rust,ignore
        // fn rem_euclid(self, rhs: Self) -> Self {
        //     let r = self % rhs;
        //     if r < 0 {
        //         // Semantically equivalent to `if rhs < 0 { r - rhs } else { r + rhs }`.
        //         // If `rhs` is not `Self::MIN`, then `r + abs(rhs)` will not overflow
        //         // and is clearly equivalent, because `r` is negative.
        //         // Otherwise, `rhs` is `Self::MIN`, then we have
        //         // `r.wrapping_add(Self::MIN.wrapping_abs())`, which evaluates
        //         // to `r.wrapping_add(Self::MIN)`, which is equivalent to
        //         // `r - Self::MIN`, which is what we wanted (and will not overflow
        //         // for negative `r`).
        //         r.wrapping_add(rhs.wrapping_abs())
        //     } else {
        //         r
        //     }
        // }
        // ```
        let r = builder.ins().srem(lhs, rhs);
        self.comment(builder.value_def(r), || {
            format!("srem_euclid({lhs}, {rhs})")
        });

        let r_lt_zero = builder.ins().icmp_imm(IntCC::SignedLessThan, r, 0);
        let abs_rhs = builder.ins().iabs(rhs);
        let r_plus_abs_rhs = builder.ins().iadd(r, abs_rhs);

        builder.ins().select(r_lt_zero, r_plus_abs_rhs, r)
    }

    pub(super) fn frem_euclid(
        &mut self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let lhs_ty = builder.value_type(lhs);
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));
        debug_assert!(lhs_ty.is_float());

        // ```rust,ignore
        // fn rem_euclid(self, rhs: f32) -> f32 {
        //     let r = self % rhs;
        //     if r < 0.0 { r + rhs.abs() } else { r }
        // }
        // ```
        let zero = if lhs_ty == types::F32 {
            builder.ins().f32const(0.0)
        } else {
            builder.ins().f64const(0.0)
        };

        let r = self.fmod(lhs, rhs, builder);
        self.comment(builder.value_def(r), || {
            format!("frem_euclid({lhs}, {rhs})")
        });

        // TODO: Should this use total float comparisons?
        let r_lt_zero = builder.ins().fcmp(FloatCC::LessThan, r, zero);
        let abs_rhs = builder.ins().fabs(rhs);
        let r_plus_abs_rhs = builder.ins().fadd(r, abs_rhs);

        builder.ins().select(r_lt_zero, r_plus_abs_rhs, r)
    }

    pub(super) fn fmod(
        &mut self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let lhs_ty = builder.value_type(lhs);
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));
        debug_assert!(lhs_ty.is_float());

        // TODO: Manually implement these functions
        // https://github.com/rust-lang/libm/blob/master/src/math/fmod.rs
        let intrinsic = if lhs_ty == types::F64 {
            "fmod"
        // https://github.com/rust-lang/libm/blob/master/src/math/fmodf.rs
        } else {
            debug_assert_eq!(lhs_ty, types::F32);
            "fmodf"
        };

        let fmod = self.imports.get(intrinsic, self.module, builder.func);
        let modulus = builder.call_fn(fmod, &[lhs, rhs]);
        self.comment(builder.value_def(modulus), || {
            format!("call {intrinsic}({lhs}, {rhs})")
        });

        modulus
    }

    pub(super) fn sdiv_checked(
        &self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let (quotient, overflowed) = self.sdiv_overflowing(lhs, rhs, builder);
        builder.ins().trapnz(overflowed, TRAP_DIV_OVERFLOW);
        quotient
    }

    pub(super) fn sdiv_overflowing(
        &self,
        lhs: Value,
        rhs: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> (Value, Value) {
        let lhs_ty = builder.value_type(lhs);
        debug_assert_eq!(lhs_ty, builder.value_type(rhs));
        debug_assert!(lhs_ty.is_int());

        let min = match lhs_ty {
            types::I8 => i8::MIN as i64,
            types::I16 => i16::MIN as i64,
            types::I32 => i32::MIN as i64,
            types::I64 => i64::MIN,
            _ => unreachable!("unsupported type {lhs_ty}"),
        };

        // if (lhs == Self::MIN) & (rhs == -1) {
        //     (lhs, true)
        // } else {
        //    (lhs / rhs, false)
        // }
        let lhs_min = builder.ins().icmp_imm(IntCC::Equal, lhs, min);
        let rhs_neg1 = builder.ins().icmp_imm(IntCC::Equal, lhs, -1);
        let overflowed = builder.ins().band(lhs_min, rhs_neg1);

        let div_block = builder.create_block();
        let after_block = builder.create_block();
        builder.append_block_param(after_block, lhs_ty);

        builder
            .ins()
            .brif(overflowed, after_block, &[lhs], div_block, &[]);
        builder.seal_current();
        builder.switch_to_block(div_block);

        let quotient = builder.ins().sdiv(lhs, rhs);
        builder.ins().jump(after_block, &[quotient]);
        builder.seal_current();
        builder.switch_to_block(after_block);

        let quotient = builder.block_params(after_block)[0];
        (quotient, overflowed)
    }
}
