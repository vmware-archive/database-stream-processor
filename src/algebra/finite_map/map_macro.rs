/// Allows easily creating [`OrdFiniteMap`](crate::algebra::OrdFiniteMap)s
#[macro_export]
macro_rules! finite_map {
    // Create an empty map
    () => {
        $crate::layers::Builder::done(<<$crate::algebra::OrdFiniteMap<_, _> as $crate::layers::Trie>::TupleBuilder as $crate::layers::TupleBuilder>::new())
    };

    // Create a map from elements
    ($($key:expr => $value:expr),+ $(,)?) => {{

        let mut builder = <<$crate::algebra::OrdFiniteMap<_, _> as $crate::layers::Trie>::TupleBuilder as $crate::layers::TupleBuilder>::with_capacity(
            $crate::count_elements!($($key),+),
        );

        $( $crate::layers::TupleBuilder::push_tuple(&mut builder, ($key, $value)); )+

        $crate::layers::Builder::done(builder)
    }};
}

/// Support macro for counting the number of map elements
#[macro_export]
#[doc(hidden)]
macro_rules! count_elements {
    (@replace $_:expr) => {
        ()
    };

    ($($_:expr),+) => {
        <[()]>::len(&[$($crate::count_elements!(@replace $_),)+])
    };
}
