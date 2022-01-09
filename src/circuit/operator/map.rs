//! Operator that applies an arbitrary function to its input.

use crate::circuit::operator_traits::{Operator, UnaryOperator};

/// Map operator.
///
/// Applies a user provided function to its input for each timestamp.
pub struct Map<F> {
    map: F,
}

impl<F> Map<F> {
    pub const fn new(map: F) -> Self
    where
        F: 'static,
    {
        Self { map }
    }
}

impl<F> Operator for Map<F>
where
    F: 'static,
{
    fn stream_start(&mut self) {}
    fn stream_end(&mut self) {}
}

impl<T1, T2, F> UnaryOperator<T1, T2> for Map<F>
where
    F: Fn(&T1) -> T2 + 'static,
{
    fn eval(&mut self, i1: &T1) -> T2 {
        (self.map)(i1)
    }
}