pub trait Combine {
    /// Combines two objects of the same type.
    fn combine(o1: Self, o2: Self) -> Self;
}
