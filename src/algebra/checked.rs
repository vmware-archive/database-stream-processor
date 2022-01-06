use num::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, One, Zero};
use std::{
    fmt::{self, Debug, Display},
    iter::{Product, Sum},
    ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Sub, SubAssign},
};

/// Ring on numeric values that panics on overflow
/// Computes exactly like any normal numeric value, but panics on overflow
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Checked<T> {
    value: T,
}

impl<T> Checked<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }

    #[inline]
    #[must_use]
    pub fn checked_add(&self, other: &Self) -> Option<Self>
    where
        T: CheckedAdd,
    {
        self.value.checked_add(&other.value).map(Self::new)
    }

    #[inline]
    #[must_use]
    pub fn checked_sub(&self, other: &Self) -> Option<Self>
    where
        T: CheckedSub,
    {
        self.value.checked_sub(&other.value).map(Self::new)
    }

    #[inline]
    #[must_use]
    pub fn checked_mul(&self, other: &Self) -> Option<Self>
    where
        T: CheckedMul,
    {
        self.value.checked_mul(&other.value).map(Self::new)
    }

    #[inline]
    #[must_use]
    pub fn checked_div(&self, other: &Self) -> Option<Self>
    where
        T: CheckedDiv,
    {
        self.value.checked_div(&other.value).map(Self::new)
    }

    #[inline]
    #[must_use]
    pub fn checked_neg(&self) -> Option<Self>
    where
        T: Zero + CheckedSub,
    {
        T::zero().checked_sub(&self.value).map(Self::new)
    }
}

impl<T> Zero for Checked<T>
where
    T: Zero + CheckedAdd,
{
    #[inline]
    #[must_use]
    fn zero() -> Self {
        Checked::new(T::zero())
    }

    #[inline]
    #[must_use]
    fn is_zero(&self) -> bool {
        self.value.is_zero()
    }
}

impl<T> One for Checked<T>
where
    T: One + CheckedMul,
{
    #[inline]
    #[must_use]
    fn one() -> Self {
        Checked::new(T::one())
    }
}

impl<T> Add for Checked<T>
where
    T: CheckedAdd,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn add(self, other: Self) -> Self::Output {
        self.checked_add(&other)
            .unwrap_or_else(|| checked_add_overflowed())
    }
}

impl<T> Add<&Self> for Checked<T>
where
    T: CheckedAdd,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn add(self, other: &Self) -> Self::Output {
        self.checked_add(other)
            .unwrap_or_else(|| checked_add_overflowed())
    }
}

impl<T> AddAssign for Checked<T>
where
    T: CheckedAdd,
{
    #[inline]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn add_assign(&mut self, other: Self) {
        *self = self
            .checked_add(&other)
            .unwrap_or_else(|| checked_add_overflowed());
    }
}

impl<T> AddAssign<&Self> for Checked<T>
where
    T: CheckedAdd,
{
    #[inline]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn add_assign(&mut self, other: &Self) {
        *self = self
            .checked_add(other)
            .unwrap_or_else(|| checked_add_overflowed());
    }
}

impl<T> Sub for Checked<T>
where
    T: CheckedSub,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn sub(self, other: Self) -> Self::Output {
        self.checked_sub(&other)
            .unwrap_or_else(|| checked_sub_underflowed())
    }
}

impl<T> Sub<&Self> for Checked<T>
where
    T: CheckedSub,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn sub(self, other: &Self) -> Self::Output {
        self.checked_sub(other)
            .unwrap_or_else(|| checked_sub_underflowed())
    }
}

impl<T> SubAssign for Checked<T>
where
    T: CheckedSub,
{
    #[inline]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn sub_assign(&mut self, other: Self) {
        *self = self
            .checked_sub(&other)
            .unwrap_or_else(|| checked_sub_underflowed());
    }
}

impl<T> SubAssign<&Self> for Checked<T>
where
    T: CheckedSub,
{
    #[inline]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn sub_assign(&mut self, other: &Self) {
        *self = self
            .checked_sub(other)
            .unwrap_or_else(|| checked_sub_underflowed());
    }
}

impl<T> Mul for Checked<T>
where
    T: CheckedMul,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn mul(self, rhs: Self) -> Self::Output {
        self.checked_mul(&rhs)
            .unwrap_or_else(|| checked_mul_overflowed())
    }
}

impl<T> Mul<&Self> for Checked<T>
where
    T: CheckedMul,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn mul(self, rhs: &Self) -> Self::Output {
        self.checked_mul(rhs)
            .unwrap_or_else(|| checked_mul_overflowed())
    }
}

impl<T> MulAssign for Checked<T>
where
    T: CheckedMul,
{
    #[inline]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn mul_assign(&mut self, rhs: Self) {
        *self = self
            .checked_mul(&rhs)
            .unwrap_or_else(|| checked_mul_overflowed());
    }
}

impl<T> MulAssign<&Self> for Checked<T>
where
    T: CheckedMul,
{
    #[inline]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn mul_assign(&mut self, rhs: &Self) {
        *self = self
            .checked_mul(rhs)
            .unwrap_or_else(|| checked_mul_overflowed());
    }
}

impl<T> Div for Checked<T>
where
    T: CheckedDiv,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn div(self, rhs: Self) -> Self::Output {
        self.checked_div(&rhs)
            .unwrap_or_else(|| checked_div_overflowed())
    }
}

impl<T> Div<&Self> for Checked<T>
where
    T: CheckedDiv,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn div(self, rhs: &Self) -> Self::Output {
        self.checked_div(rhs)
            .unwrap_or_else(|| checked_div_overflowed())
    }
}

impl<T> DivAssign for Checked<T>
where
    T: CheckedDiv,
{
    #[inline]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn div_assign(&mut self, rhs: Self) {
        *self = self
            .checked_div(&rhs)
            .unwrap_or_else(|| checked_div_overflowed());
    }
}

impl<T> DivAssign<&Self> for Checked<T>
where
    T: CheckedDiv,
{
    #[inline]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn div_assign(&mut self, rhs: &Self) {
        *self = self
            .checked_div(rhs)
            .unwrap_or_else(|| checked_div_overflowed());
    }
}

impl<T> Neg for Checked<T>
where
    T: CheckedSub + Zero,
{
    type Output = Self;

    #[inline]
    #[must_use]
    #[track_caller]
    #[allow(clippy::redundant_closure)]
    fn neg(self) -> Self::Output {
        self.checked_neg()
            .unwrap_or_else(|| checked_neg_overflowed())
    }
}

impl<T> Sum for Checked<T>
where
    T: Zero + CheckedAdd,
{
    #[inline]
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        iter.fold(Checked::zero(), |a, b| a + b)
    }
}

impl<'a, T> Sum<&'a Self> for Checked<T>
where
    T: Zero + CheckedAdd,
{
    #[inline]
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = &'a Self>,
    {
        iter.fold(Checked::zero(), |a, b| a + b)
    }
}

impl<T> Product for Checked<T>
where
    T: Zero + CheckedMul + CheckedAdd,
{
    #[inline]
    fn product<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        iter.fold(Checked::zero(), |a, b| a * b)
    }
}

impl<'a, T> Product<&'a Self> for Checked<T>
where
    T: Zero + CheckedMul + CheckedAdd,
{
    #[inline]
    fn product<I>(iter: I) -> Self
    where
        I: Iterator<Item = &'a Self>,
    {
        iter.fold(Checked::zero(), |a, b| a * b)
    }
}

impl<T> From<T> for Checked<T> {
    #[inline]
    fn from(value: T) -> Self {
        Self { value }
    }
}

impl<T> Debug for Checked<T>
where
    T: Debug,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.value, f)
    }
}

impl<T> Display for Checked<T>
where
    T: Display,
{
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.value, f)
    }
}

#[cold]
#[track_caller]
#[inline(never)]
fn checked_add_overflowed() -> ! {
    panic!("attempted to add with overflow")
}

#[cold]
#[track_caller]
#[inline(never)]
fn checked_sub_underflowed() -> ! {
    panic!("attempted to subtract with underflow")
}

#[cold]
#[track_caller]
#[inline(never)]
fn checked_mul_overflowed() -> ! {
    panic!("attempted to multiply with overflow")
}

#[cold]
#[track_caller]
#[inline(never)]
fn checked_div_overflowed() -> ! {
    panic!("attempted to divide with overflow")
}

#[cold]
#[track_caller]
#[inline(never)]
fn checked_neg_overflowed() -> ! {
    panic!("attempted to negate with overflow")
}

#[cfg(test)]
mod checked_integer_ring_tests {
    use super::{Checked, One, Zero};
    use std::ops::Neg;

    type CheckedI64 = Checked<i64>;
    type CheckedU64 = Checked<u64>;

    #[test]
    fn fixed_integer_tests() {
        assert_eq!(0, CheckedI64::zero().into_inner());
        assert_eq!(1, CheckedI64::one().into_inner());

        let two = CheckedI64::one() + CheckedI64::one();
        assert_eq!(2, two.into_inner());
        assert_eq!(-2, two.neg().into_inner());
        assert_eq!(-4, (two * -two).into_inner());

        let mut three = two;
        three += CheckedI64::new(1);
        assert_eq!(3, three.into_inner());
        assert!(!three.is_zero());
    }

    #[test]
    #[should_panic = "attempted to add with overflow"]
    fn overflow_add() {
        let _ = CheckedI64::new(i64::MAX) + CheckedI64::one();
    }

    #[test]
    #[should_panic = "attempted to subtract with underflow"]
    fn overflow_sub() {
        let _ = CheckedU64::zero() - CheckedU64::one();
    }

    #[test]
    #[should_panic = "attempted to multiply with overflow"]
    fn overflow_mul() {
        let _ = CheckedI64::new(i64::MAX) * CheckedI64::new(2);
    }

    #[test]
    #[should_panic = "attempted to divide with overflow"]
    fn overflow_div() {
        let _ = CheckedI64::new(i64::MIN) / CheckedI64::new(-1);
    }

    #[test]
    #[should_panic = "attempted to negate with overflow"]
    fn overflow_neg() {
        let _ = -CheckedU64::new(u64::MAX);
    }
}
