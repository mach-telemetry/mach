use mach::{
    tags::Tags,
    id::SeriesId,
};
use dashmap::DashMap;
use std::collections::HashSet;
use regex::Regex;

#[derive(Clone)]
pub struct TagIndex {
    map: DashMap<String, HashSet<Tags>>,
}

impl TagIndex {
    pub fn new() -> Self {
        TagIndex {
            map: DashMap::new()
        }
    }

    pub fn insert(&self, tags: Tags) {
        let series_id = tags.id();
        for tag in tags.data() {
            let to_index = format!("{}={}", tag.0, tag.1);
            self.map.entry(to_index).or_insert_with(|| { HashSet::new() }).insert(tags.clone());
        }
    }

    pub fn search(&self, re: &Regex) -> HashSet<Tags> {
        let mut set = HashSet::new();
        for item in self.map.iter() {
            let k = item.key();
            if re.is_match(k.as_str()) {
                set = set.union(item.value()).map(|x| {
                    x
                }).cloned().collect();
            }
        }
        set
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn simple_test() {
        let index = TagIndex::new();

        let mut tags1 = HashMap::new();
        tags1.insert(String::from("foo"), String::from("bar"));
        tags1.insert(String::from("bar"), String::from("baz"));
        let tags1 = Tags::from(tags1);
        index.insert(tags1.clone());

        let mut tags2 = HashMap::new();
        tags2.insert(String::from("alice"), String::from("bob"));
        tags2.insert(String::from("bar"), String::from("baz"));
        let tags2 = Tags::from(tags2);
        index.insert(tags2.clone());

        let result = index.search(&Regex::new(r"^fo.=ba.").unwrap());
        println!("{:?}", result);
        assert!(result.contains(&tags1));
        assert!(!result.contains(&tags2));

        let result = index.search(&Regex::new(r"^ba.=ba.").unwrap());
        println!("{:?}", result);
        assert!(result.contains(&tags1));
        assert!(result.contains(&tags2));
    }
}

