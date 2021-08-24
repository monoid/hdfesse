use std::ops::DerefMut;
use reffers::arc::{Strong, RefMut};

pub trait Holder<'a, T: 'a> {
    type Guard: DerefMut<Target=T> + 'a;

    fn aquire(&'a mut self) -> Self::Guard;
}

pub struct Owned<T>(T);

impl<'a, T: 'a> Holder<'a, T> for Owned<T> {
    type Guard = &'a mut T;

    fn aquire(&'a mut self) -> Self::Guard {
        &mut self.0
    }
}

impl<T> Owned<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

pub struct Shared<T>(Strong<T>);

impl<'a, T: 'a> Holder<'a, T> for Shared<T> {
    type Guard = RefMut<T>;

    fn aquire(&'a mut self) -> Self::Guard {
        self.0.get_refmut()
    }
}
