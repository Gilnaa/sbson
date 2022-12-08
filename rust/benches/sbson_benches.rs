use criterion::{criterion_group, criterion_main, Criterion};
use sbson::BorrowedCursor;

const DOC: &[u8] = b"\x03\x04\x00\x00\x00%\x00\x00\x00o\x00\x00\x00\'\x00\x00\x00y\x00\x00\x00-\x00\x00\x00\xa7\x00\x00\x003\x00\x00\x00\xbf\x00\x00\x003\x00BLARG\x00FLORP\x00help me i\'m trapped in a format factory help me before they\x00\x05beep boop\x04\x05\x00\x00\x00\x19\x00\x00\x00\"\x00\x00\x00+\x00\x00\x00,\x00\x00\x00-\x00\x00\x00\x12\x01\x00\x00\x00\x00\x00\x00\x00\x12\x02\x00\x00\x00\x00\x00\x00\x00\t\x08\n\x03\x01\x00\x00\x00\r\x00\x00\x00\x0f\x00\x00\x00X\x00\x12\xff\x00\x00\x00\x00\x00\x00\x00\x02...\x00";

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("goto", |b| {
        b.iter(|| {
            let cur = BorrowedCursor::new(DOC).unwrap();
            cur.get_value_by_key("BLARG").unwrap().get_value_by_index(3).unwrap().parse_bool().unwrap();
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_plots();
    targets = criterion_benchmark
);
criterion_main!(benches);