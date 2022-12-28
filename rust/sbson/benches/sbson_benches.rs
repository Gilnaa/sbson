use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sbson::{Cursor, PathSegment};

const GOTO_TREE: &[u8] = include_bytes!("../../../test_vectors/goto.sbson");
const GOTO_TREE_PHF: &[u8] = include_bytes!("../../../test_vectors/goto_phf.sbson");

fn bench_goto_item(c: &mut Criterion) {
    let cur_borrow: Cursor<&[u8]> = Cursor::new(GOTO_TREE).unwrap();
    let top_borrow = cur_borrow.get_value_by_key("top").unwrap();

    let cur_borrow_chd: Cursor<&[u8]> = Cursor::new(GOTO_TREE_PHF).unwrap();
    let top_borrow_chd = cur_borrow_chd.get_value_by_key("top").unwrap();

    let mut group = c.benchmark_group("goto");

    // The test vector has a map with 8000 items named `item_{i}`.
    // Since keying into the map is a O(log2(N)), we're measuring access at various points.
    //
    // 4095 is the root node when encoding using Eytzinger.
    // Stepping by a prime number to avoid taking numbers that will match some other bin-search pattern.
    for i in (0..8000).step_by(71).chain([4095]) {
        let item_name = format!("item_{i:04}");

        group.bench_function(BenchmarkId::new("descend_eytzinger_one_by_one", i), |b| {
            b.iter(|| {
                let integer = top_borrow
                    .get_value_by_key(&item_name)
                    .unwrap()
                    .get_value_by_key("something")
                    .unwrap()
                    .get_value_by_index(3)
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            });
        });

        group.bench_function(BenchmarkId::new("descend_chd_one_by_one", i), |b| {
            b.iter(|| {
                let integer = top_borrow_chd
                    .get_value_by_key(&item_name)
                    .unwrap()
                    .get_value_by_key("something")
                    .unwrap()
                    .get_value_by_index(3)
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            });
        });

        group.bench_function(BenchmarkId::new("descend_eytzinger_goto", i), |b| {
            b.iter(|| {
                let integer = top_borrow
                    .goto(
                        [
                            PathSegment::Key(&item_name),
                            PathSegment::Key("something"),
                            PathSegment::Index(3),
                        ]
                        .into_iter(),
                    )
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            });
        });

        group.bench_function(BenchmarkId::new("descend_chd_goto", i), |b| {
            b.iter(|| {
                let integer = top_borrow_chd
                    .goto(
                        [
                            PathSegment::Key(&item_name),
                            PathSegment::Key("something"),
                            PathSegment::Index(3),
                        ]
                        .into_iter(),
                    )
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            });
        });
    }
}

fn bench_goto_all_items(c: &mut Criterion) {
    let item_names: Vec<_> = (0..8000).map(|i| format!("item_{i:04}")).collect();
    let mut group = c.benchmark_group("goto_all_items");

    let goto_tree_arc: Arc<[u8]> = GOTO_TREE.into();
    let goto_tree_phf_arc: Arc<[u8]> = GOTO_TREE_PHF.into();

    let cur_borrow: Cursor<&[u8]> = Cursor::new(goto_tree_arc.as_ref()).unwrap();
    let top_borrow = cur_borrow.get_value_by_key("top").unwrap();

    let cur_borrow_chd: Cursor<&[u8]> = Cursor::new(goto_tree_phf_arc.as_ref()).unwrap();
    let top_borrow_chd = cur_borrow_chd.get_value_by_key("top").unwrap();

    let cur_arc_chd: Cursor<Arc<[u8]>> = Cursor::new(goto_tree_phf_arc.clone()).unwrap();
    let top_arc_chd = cur_arc_chd.get_value_by_key("top").unwrap();

    group.bench_function("descend_eytzinger_one_by_one", |b| {
        b.iter(|| {
            for item_name in item_names.iter() {
                let integer = top_borrow
                    .get_value_by_key(&item_name)
                    .unwrap()
                    .get_value_by_key("something")
                    .unwrap()
                    .get_value_by_index(3)
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            }
        });
    });

    group.bench_function("descend_eytzinger_goto", |b| {
        b.iter(|| {
            for item_name in item_names.iter() {
                let integer = top_borrow
                    .goto(
                        [
                            PathSegment::Key(&item_name),
                            PathSegment::Key("something"),
                            PathSegment::Index(3),
                        ]
                        .into_iter(),
                    )
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            }
        });
    });

    group.bench_function("descend_chd_one_by_one", |b| {
        b.iter(|| {
            for item_name in item_names.iter() {
                let integer = top_borrow_chd
                    .get_value_by_key(&item_name)
                    .unwrap()
                    .get_value_by_key("something")
                    .unwrap()
                    .get_value_by_index(3)
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            }
        });
    });

    group.bench_function("descend_chd_goto", |b| {
        b.iter(|| {
            for item_name in item_names.iter() {
                let integer = top_borrow_chd
                    .goto(
                        [
                            PathSegment::Key(&item_name),
                            PathSegment::Key("something"),
                            PathSegment::Index(3),
                        ]
                        .into_iter(),
                    )
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            }
        });
    });

    group.bench_function("descend_arc_chd_one_by_one", |b| {
        b.iter(|| {
            for item_name in item_names.iter() {
                let integer = top_arc_chd
                    .get_value_by_key(&item_name)
                    .unwrap()
                    .get_value_by_key("something")
                    .unwrap()
                    .get_value_by_index(3)
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            }
        });
    });

    group.bench_function("descend_arc_chd_goto", |b| {
        b.iter(|| {
            for item_name in item_names.iter() {
                let integer = top_arc_chd
                    .goto(
                        [
                            PathSegment::Key(&item_name),
                            PathSegment::Key("something"),
                            PathSegment::Index(3),
                        ]
                        .into_iter(),
                    )
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            }
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_plots();
    targets = bench_goto_item, bench_goto_all_items
);
criterion_main!(benches);
