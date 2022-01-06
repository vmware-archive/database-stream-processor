/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) 2021 VMware, Inc
*/

//! This module contains declarations of abstract algebraic concepts:
//! monoids, groups, rings, etc.

mod checked;
pub mod zset;

pub use checked::Checked;

use num::{One, Zero};
use std::{
    fmt::{Debug, Display},
    ops::{Add, AddAssign, Mul, Neg},
};

/// A type with an associative addition and a zero.
/// We trust the implementation to have an associative addition.
/// This contains methods 'is_zero', 'zero' and 'add'
pub trait MonoidValue: Clone + Eq + 'static + Add<Output = Self> + Zero + AddAssign {}

/// Default implementation for all types that have an addition.
impl<T> MonoidValue for T where T: Clone + Eq + 'static + Add<Output = Self> + Zero + AddAssign {}

/// A `MonoidValue` with negation.
/// In addition we expect all our groups to be commutative.
/// This adds the 'neg' method to the MonoidValue methods.
pub trait GroupValue: MonoidValue + Neg<Output = Self> {}

/// Default implementation for all types that have the required traits.
impl<T> GroupValue for T where
    T: Clone + Eq + 'static + Add<Output = Self> + Zero + AddAssign + Neg<Output = Self>
{
}

/// A Group with a multiplication operation
/// This adds the 'mul' method
pub trait RingValue: GroupValue + Mul<Output = Self> + One {}

/// Default implementation for all types that have the required traits.
impl<T> RingValue for T where
    T: Clone
        + Eq
        + 'static
        + Add<Output = Self>
        + Zero
        + AddAssign
        + Neg<Output = Self>
        + Mul<Output = Self>
        + One
{
}

/// A ring where elements can be compared with zero
pub trait ZRingValue: RingValue {
    /// True if value is greater or equal to zero
    fn ge0(&self) -> bool;
}

/// Default implementation for all types that have the required traits.
impl<T> ZRingValue for T
where
    T: Clone
        + Eq
        + 'static
        + Add<Output = Self>
        + Zero
        + AddAssign
        + Neg<Output = Self>
        + Mul<Output = Self>
        + One
        + Ord,
{
    fn ge0(&self) -> bool {
        *self >= Self::zero()
    }
}

#[cfg(test)]
mod integer_ring_tests {
    use super::{One, Zero};

    #[test]
    fn fixed_integer_tests_i64() {
        assert_eq!(0, i64::zero());
        assert_eq!(1, i64::one());

        let two = i64::one() + i64::one();
        assert_eq!(2, two);
        assert_eq!(-2, -two);
        assert_eq!(-4, two * -two);
    }
}
