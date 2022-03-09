struct Node {
    data: [u8, 1024],
    next: Box<Node>,
}

pub struct Vec {
    head: Node
}

impl Vec {
}
