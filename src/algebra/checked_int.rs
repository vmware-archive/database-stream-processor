use crate::algebra::{AddAssignByRef, AddByRef, HasOne, HasZero, MulByRef, MulByWeight, NegByRef};
use num::{traits::CheckedNeg, CheckedAdd, CheckedMul};
use std::{
    cmp::Ordering,
    fmt::{Debug, Display, Error, Formatter},
    ops::{Add, AddAssign, Neg},
};

/// Ring on numeric values that panics on overflow
/// Computes exactly like any signed numeric value, but panics on overflow
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct CheckedInt<T> {
    value: T,
}

impl<T> CheckedInt<T> {
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Add for CheckedInt<T>
where
    T: CheckedAdd,
{
    type Output = Self;

    fn add(self, other: Self) -> Self {
        // intentional panic on overflow
        Self {
            value: self.value.checked_add(&other.value).expect("overflow"),
        }
    }
}

impl<T> AddByRef for CheckedInt<T>
where
    T: CheckedAdd,
{
    fn add_by_ref(&self, other: &Self) -> Self {
        // intentional panic on overflow
        Self {
            value: self.value.checked_add(&other.value).expect("overflow"),
        }
    }
}

impl<T> AddAssign for CheckedInt<T>
where
    T: CheckedAdd,
{
    fn add_assign(&mut self, other: Self) {
        self.value = self.value.checked_add(&other.value).expect("overflow")
    }
}

impl<T> AddAssignByRef for CheckedInt<T>
where
    T: CheckedAdd,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.value = self.value.checked_add(&other.value).expect("overflow")
    }
}

impl<T> MulByRef for CheckedInt<T>
where
    T: CheckedMul,
{
    type Output = T;

    fn mul_by_ref(&self, rhs: &Self) -> Self {
        // intentional panic on overflow
        Self {
            value: self.value.checked_mul(&rhs.value).expect("overflow"),
        }
    }
}

impl<T> NegByRef for CheckedInt<T>
where
    T: CheckedNeg,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            // intentional panic on overflow
            value: self.value.checked_neg().expect("overflow"),
        }
    }
}

impl<T> Neg for CheckedInt<T>
where
    T: CheckedNeg,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            // intentional panic on overflow
            value: self.value.checked_neg().expect("overflow"),
        }
    }
}

impl<T> HasZero for CheckedInt<T>
where
    T: num::traits::Zero + CheckedAdd,
{
    fn is_zero(&self) -> bool {
        T::is_zero(&self.value)
    }

    fn zero() -> Self {
        Self::new(T::zero())
    }
}

impl<T> HasOne for CheckedInt<T>
where
    T: num::traits::One + CheckedMul,
{
    fn one() -> Self {
        Self::new(T::one())
    }
}

impl<T> PartialEq<T> for CheckedInt<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &T) -> bool {
        &self.value == other
    }
}

impl<T> PartialOrd<T> for CheckedInt<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        self.value.partial_cmp(other)
    }
}

impl<T> From<T> for CheckedInt<T> {
    fn from(value: T) -> Self {
        Self { value }
    }
}

impl<T> Debug for CheckedInt<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.value.fmt(f)
    }
}

impl<T> Display for CheckedInt<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.value.fmt(f)
    }
}

impl<D, T> MulByWeight<CheckedInt<T>> for D
where
    D: MulByRef<T, Output = D>,
{
    fn weigh(&self, w: &CheckedInt<T>) -> Self {
        self.mul_by_ref(&w.value) as Self
    }
}

#[cfg(test)]
mod checked_integer_ring_tests {
    use super::{AddAssignByRef, AddByRef, CheckedInt, HasOne, HasZero, MulByRef, NegByRef};
    use crate::algebra::MulByWeight;

    type CheckedI64 = CheckedInt<i64>;

    #[test]
    fn fixed_integer_tests() {
        assert_eq!(0i64, CheckedI64::zero().into_inner());
        assert_eq!(1i64, CheckedI64::one().into_inner());

        let two = CheckedI64::one().add_by_ref(&CheckedI64::one());
        assert_eq!(2i64, two.into_inner());
        assert_eq!(-2i64, two.neg_by_ref().into_inner());
        assert_eq!(-4i64, two.mul_by_ref(&two.neg_by_ref()).into_inner());

        let mut three = two;
        three.add_assign_by_ref(&CheckedI64::from(1i64));
        assert_eq!(3i64, three.into_inner());
        assert!(!three.is_zero());
        assert_eq!(6i64, 2i64.weigh(&three));
    }

    #[test]
    #[should_panic]
    fn overflow_test() {
        let max = CheckedI64::from(i64::MAX);
        let _ = max.add_by_ref(&CheckedI64::one());
    }
}
