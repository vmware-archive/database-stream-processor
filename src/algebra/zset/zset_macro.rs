/// Allows easily creating [`OrdIndexedZSet`](crate::algebra::OrdIndexedZSet)s
#[macro_export]
macro_rules! indexed_zset {
    ( $($key:expr => { $($value:expr => $weight:expr),* }),* $(,)?) => {{
        let mut builder = <<$crate::algebra::OrdIndexedZSet<_, _, _> as $crate::layers::Trie>::TupleBuilder as $crate::layers::TupleBuilder>::new();

        $( $( $crate::layers::TupleBuilder::push_tuple(&mut builder, ($key, ($value, $weight))); )* )*

        $crate::layers::Builder::done(builder)
    }};
}
