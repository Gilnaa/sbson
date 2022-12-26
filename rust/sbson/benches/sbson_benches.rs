use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sbson::Cursor;

const GOTO_TREE: &[u8] = include_bytes!("../../../test_vectors/goto.sbson");
const GOTO_TREE_PHF: &[u8] = include_bytes!("../../../test_vectors/goto_phf.sbson");

fn criterion_benchmark(c: &mut Criterion) {
    // Turn it into an Arc once at the start of the test.
    // We're not interested in the cost of copying the whole buffer into the Arc.
    let goto_tree_arc: Arc<[u8]> = GOTO_TREE.into();
    let goto_tree_phf_arc: Arc<[u8]> = GOTO_TREE_PHF.into();
    let cur_borrow = Cursor::new(goto_tree_arc.as_ref()).unwrap();
    let top_borrow = cur_borrow.get_value_by_key("top").unwrap();
    let cur_arc = Cursor::new(goto_tree_arc.clone()).unwrap();
    let top_arc = cur_arc.get_value_by_key("top").unwrap();

    let cur_borrow_chd = Cursor::new(&goto_tree_phf_arc).unwrap();
    let top_borrow_chd = cur_borrow_chd.get_value_by_key("top").unwrap();
    let cur_arc_chd = Cursor::new(goto_tree_phf_arc.clone()).unwrap();
    let top_arc_chd = cur_arc_chd.get_value_by_key("top").unwrap();

    // let cached = top_arc.cache_map().unwrap();

    let mut group = c.benchmark_group("goto");

    // The test vector has a map with 8000 items named `item_{i}`.
    // Since keying into the map is a O(log2(N)), we're measuring access at various points.
    for i in (0..8000).step_by(80) {
        let item_name = format!("item_{i:04}");

        group.bench_function(BenchmarkId::new("goto_borrow", i), |b| {
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

        group.bench_function(BenchmarkId::new("goto_borrow_chd", i), |b| {
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

        group.bench_function(BenchmarkId::new("goto_borrow_index", i), |b| {
            b.iter(|| {
                let integer = top_borrow
                    .get_value_by_index(i)
                    .unwrap()
                    .get_value_by_key("something")
                    .unwrap()
                    .get_value_by_index(3)
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            });
        });

        group.bench_function(BenchmarkId::new("goto_arc", i), |b| {
            b.iter(|| {
                let integer = top_arc
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

        group.bench_function(BenchmarkId::new("goto_arc_chd", i), |b| {
            b.iter(|| {
                let integer = top_arc_chd
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

        group.bench_function(BenchmarkId::new("goto_arc_index", i), |b| {
            b.iter(|| {
                let integer = top_arc
                    .get_value_by_index(i)
                    .unwrap()
                    .get_value_by_key("something")
                    .unwrap()
                    .get_value_by_index(3)
                    .unwrap()
                    .get_element_type();
                black_box(integer);
            });
        });

        // group.bench_function(BenchmarkId::new("goto_arc_cached", i), |b| {
        //     b.iter(|| {
        //         let integer = cached
        //             .get_value_by_key(&item_name)
        //             .unwrap()
        //             .get_value_by_key("something")
        //             .unwrap()
        //             .get_value_by_index(3)
        //             .unwrap()
        //             .get_element_type();
        //         black_box(integer);
        //     });
        // });
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_plots();
    targets = criterion_benchmark
);
criterion_main!(benches);
