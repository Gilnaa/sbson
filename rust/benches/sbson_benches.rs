use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sbson::{ArcCursor, BorrowedCursor};

const GOTO_TREE: &[u8] = include_bytes!("../../test_vectors/goto.sbson");

fn criterion_benchmark(c: &mut Criterion) {
    // Turn it into an Arc once at the start of the test.
    // We're not interested in the cost of copying the whole buffer into the Arc.
    let goto_tree_arc: Arc<[u8]> = GOTO_TREE.into();

    let mut group = c.benchmark_group("goto");

    // The test vector has a map with 8000 items named `item_{i}`.
    // Since keying into the map is a O(log2(N)), we're measuring access at various points.
    for i in (0..8000).step_by(80) {
        let item_name = format!("item_{i}");

        group.bench_function(BenchmarkId::new("goto_borrow", i), |b| {
            b.iter(|| {
                let cur = BorrowedCursor::new(&goto_tree_arc).unwrap();
                let integer = cur
                    .get_value_by_key("top")
                    .unwrap()
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

        group.bench_function(BenchmarkId::new("goto_arc", i), |b| {
            b.iter(|| {
                let cur = ArcCursor::new(goto_tree_arc.clone()).unwrap();
                let integer = cur
                    .get_value_by_key("top")
                    .unwrap()
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
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_plots();
    targets = criterion_benchmark
);
criterion_main!(benches);
