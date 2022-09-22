//! Binary operator that applies an arbitrary binary function to its inputs.

use crate::circuit::{
    operator_traits::{BinaryOperator, Operator, OperatorLocation},
    Circuit, OwnershipPreference, Scope, Stream,
};
use std::{borrow::Cow, panic::Location};

impl<P, T1> Stream<Circuit<P>, T1>
where
    P: Clone + 'static,
    T1: Clone + 'static,
{
    /// Apply a user-provided binary function to its inputs at each timestamp.
    #[track_caller]
    pub fn apply2<F, T2, T3>(
        &self,
        other: &Stream<Circuit<P>, T2>,
        func: F,
    ) -> Stream<Circuit<P>, T3>
    where
        T2: Clone + 'static,
        T3: Clone + 'static,
        F: Fn(&T1, &T2) -> T3 + 'static,
    {
        self.circuit()
            .add_binary_operator(Apply2::new(func, Location::caller()), self, other)
    }

    /// Apply a user-provided binary function to its inputs at each timestamp,
    /// consuming the first input.
    #[track_caller]
    pub fn apply2_owned<F, T2, T3>(
        &self,
        other: &Stream<Circuit<P>, T2>,
        func: F,
    ) -> Stream<Circuit<P>, T3>
    where
        T2: Clone + 'static,
        T3: Clone + 'static,
        F: Fn(T1, &T2) -> T3 + 'static,
    {
        self.circuit().add_binary_operator_with_preference(
            Apply2Owned::new(func, Location::caller()),
            self,
            other,
            OwnershipPreference::STRONGLY_PREFER_OWNED,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

/// Applies a user-provided binary function to its inputs at each timestamp.
pub struct Apply2<F> {
    func: F,
    location: &'static Location<'static>,
}

impl<F> Apply2<F> {
    pub const fn new(func: F, location: &'static Location<'static>) -> Self
    where
        F: 'static,
    {
        Self { func, location }
    }
}

impl<F> Operator for Apply2<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Apply2")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // TODO: either change `F` type to `Fn` from `FnMut` or
        // parameterize the operator with custom fixed point check.
        unimplemented!();
    }
}

impl<T1, T2, T3, F> BinaryOperator<T1, T2, T3> for Apply2<F>
where
    F: Fn(&T1, &T2) -> T3 + 'static,
{
    fn eval(&mut self, i1: &T1, i2: &T2) -> T3 {
        (self.func)(i1, i2)
    }
}

/// Applies a user-provided binary function to its inputs at each timestamp,
/// consuming the first input.
pub struct Apply2Owned<F> {
    func: F,
    location: &'static Location<'static>,
}

impl<F> Apply2Owned<F> {
    pub const fn new(func: F, location: &'static Location<'static>) -> Self
    where
        F: 'static,
    {
        Self { func, location }
    }
}

impl<F> Operator for Apply2Owned<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Apply2Owned")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // TODO: either change `F` type to `Fn` from `FnMut` or
        // parameterize the operator with custom fixed point check.
        unimplemented!();
    }
}

impl<T1, T2, T3, F> BinaryOperator<T1, T2, T3> for Apply2Owned<F>
where
    F: Fn(T1, &T2) -> T3 + 'static,
{
    fn eval(&mut self, _i1: &T1, _i2: &T2) -> T3 {
        panic!("Apply2Owned: owned input expected")
    }

    fn eval_owned_and_ref(&mut self, i1: T1, i2: &T2) -> T3 {
        (self.func)(i1, i2)
    }

    fn eval_owned(&mut self, i1: T1, i2: T2) -> T3 {
        (self.func)(i1, &i2)
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::STRONGLY_PREFER_OWNED,
            OwnershipPreference::INDIFFERENT,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{operator::Generator, Circuit};
    use std::vec;

    #[test]
    fn apply2_test() {
        let circuit = Circuit::build(move |circuit| {
            let mut inputs1 = vec![1, 2, 3].into_iter();
            let mut inputs2 = vec![-1, -2, -3].into_iter();

            let source1 = circuit.add_source(Generator::new(move || inputs1.next().unwrap()));
            let source2 = circuit.add_source(Generator::new(move || inputs2.next().unwrap()));

            source1
                .apply2(&source2, |x, y| *x + *y)
                .inspect(|z| assert_eq!(*z, 0));
        })
        .unwrap()
        .0;

        for _ in 0..3 {
            circuit.step().unwrap();
        }
    }
}
