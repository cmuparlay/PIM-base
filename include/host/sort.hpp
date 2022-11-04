#pragma once
#include <parlay/primitives.h>
#include <parlay/parallel.h>
#include <parlay/internal/sequence_ops.h>
#include "macro.h"

using namespace std;
using namespace parlay;

template <typename assignment_tag, typename InSeq, typename OffsetIterator, typename KeySeq, typename BufferIterator, typename LocationIterator>
void fill_task_to_buffer(InSeq In, KeySeq Keys, OffsetIterator offsets, size_t num_buckets, slice<BufferIterator, BufferIterator> buffers, slice<LocationIterator, LocationIterator> locations) {
  // copy to local offsets to avoid false sharing
  using LocationType = typename std::iterator_traits<LocationIterator>::value_type;
  auto local_offsets = sequence<LocationType>::uninitialized(num_buckets);
  for (size_t i = 0; i < num_buckets; i++) local_offsets[i] = offsets[i];
  for (size_t j = 0; j < In.size(); j++) {
    LocationType k = local_offsets[Keys[j]]++;
    auto ptr = &(buffers[Keys[j]][k]);
    // #if defined(__GNUC__) || defined(__clang__)
    //    __builtin_prefetch (((char*) ptr) + 64);
    // #endif
    *ptr = In[j];
    locations[j] = k;
  }
}

template <bool id_from_func, typename TaskIterator, typename Id,
          typename BufferIterator, typename LocationIterator,
          typename CountIterator>
void inner_sort_task(slice<TaskIterator, TaskIterator> In, Id id,
                     slice<BufferIterator, BufferIterator> buffers,
                     slice<LocationIterator, LocationIterator> location,
                     slice<CountIterator, CountIterator> bucket_counts) {
    if constexpr (id_from_func) {
        int n = In.size();
        auto f = [&](size_t i) { return id(In[i]); };
        auto key_seq = delayed_seq<size_t>(n, f);
        auto keys = make_slice(key_seq);
        inner_sort_task<false>(In, keys, buffers, location, bucket_counts);
        return;
    } else {
        using s_size_t = uint32_t;
        using T = typename slice<TaskIterator, TaskIterator>::value_type;
        size_t num_buckets = buffers.size();

        size_t n = In.size();

        size_t num_blocks =
            1 + n * sizeof(T) / std::max<size_t>(num_buckets * 500, 5000);
        size_t block_size = ((n - 1) / num_blocks) + 1;

        auto Keys = id;

        size_t m = num_blocks * num_buckets;
        auto counts = sequence<s_size_t>::uninitialized(m);
        // sort each block
        parallel_for(
            0, num_blocks,
            [&](size_t i) {
                size_t start = (std::min)(i * block_size, n);
                size_t end = (std::min)(start + block_size, n);
                internal::seq_count_(
                    In.cut(start, end), Keys.cut(start, end),
                    counts.begin() + i * num_buckets, num_buckets);
            },
            1);

        auto dest_offsets =
            sequence<uint32_t>::uninitialized(num_blocks * num_buckets);

        parallel_for(
            0, num_buckets,
            [&](size_t i) {
                auto v = 0;
                for (size_t j = 0; j < num_blocks; j++) {
                    dest_offsets[j * num_buckets + i] = v;
                    v += counts[j * num_buckets + i];
                }
                bucket_counts[i] = v;
            },
            1 + 1024 / num_blocks);

        parallel_for(
            0, num_blocks,
            [&](size_t i) {
                size_t start = (std::min)(i * block_size, n);
                size_t end = (std::min)(start + block_size, n);
                fill_task_to_buffer<parlay::copy_assign_tag>(
                    In.cut(start, end), Keys.cut(start, end),
                    dest_offsets.begin() + i * num_buckets, num_buckets,
                    buffers, location.cut(start, end));
            },
            1);
        return;
    }
}

template <typename assignment_tag, typename InSeq, typename OffsetIterator,
          typename KeySeq, typename OutSeq, typename LocationIterator>
void fill_task_to_array(InSeq In, KeySeq Keys, OffsetIterator offsets,
                        size_t num_buckets, OutSeq array,
                        slice<LocationIterator, LocationIterator> locations) {
    // copy to local offsets to avoid false sharing
    using LocationType =
        typename std::iterator_traits<LocationIterator>::value_type;
    auto local_offsets = sequence<LocationType>::uninitialized(num_buckets);
    for (size_t i = 0; i < num_buckets; i++) local_offsets[i] = offsets[i];
    for (size_t j = 0; j < In.size(); j++) {
        LocationType k = local_offsets[Keys[j]]++;
        auto ptr = &(array[k]);
// needs to be made portable
#if defined(__GNUC__) || defined(__clang__)
        __builtin_prefetch(((char*)ptr) + 64);
#endif
        *ptr = In[j];
        locations[j] = k;
    }
}

template <typename assignment_tag, typename s_size_t, typename InIterator,
          typename OutIterator, typename KeyIterator, typename LocationIterator>
std::pair<sequence<size_t>, bool> count_sort_task(
    slice<InIterator, InIterator> In, slice<OutIterator, OutIterator> Out,
    slice<KeyIterator, KeyIterator> Keys, size_t num_buckets,
    slice<LocationIterator, LocationIterator> location) {
    using T = typename slice<InIterator, InIterator>::value_type;
    size_t n = In.size();
    bool is_nested = false;

    // pick number of blocks for sufficient parallelism but to make sure
    // cost on counts is not to high (i.e. bucket upper).
    size_t num_blocks =
        1 + n * sizeof(T) / std::max<size_t>(num_buckets * 500, 5000);

    size_t block_size = ((n - 1) / num_blocks) + 1;
    size_t m = num_blocks * num_buckets;

    auto counts = sequence<s_size_t>::uninitialized(m);

    // sort each block
    parallel_for(
        0, num_blocks,
        [&](size_t i) {
            size_t start = (std::min)(i * block_size, n);
            size_t end = (std::min)(start + block_size, n);
            internal::seq_count_(In.cut(start, end),
                                 make_slice(Keys).cut(start, end),
                                 counts.begin() + i * num_buckets, num_buckets);
        },
        1, is_nested);

    auto bucket_offsets = sequence<size_t>::uninitialized(num_buckets + 1);
    parallel_for(
        0, num_buckets,
        [&](size_t i) {
            size_t v = 0;
            for (size_t j = 0; j < num_blocks; j++)
                v += counts[j * num_buckets + i];
            bucket_offsets[i] = v;
        },
        1 + 1024 / num_blocks);
    bucket_offsets[num_buckets] = 0;

    // if all in one bucket, then no need to sort
    [[maybe_unused]] size_t total =
        scan_inplace(make_slice(bucket_offsets), addm<size_t>());

    auto dest_offsets =
        sequence<uint32_t>::uninitialized(num_blocks * num_buckets);
    parallel_for(
        0, num_buckets,
        [&](size_t i) {
            auto v = bucket_offsets[i];  // + Out.begin();
            for (size_t j = 0; j < num_blocks; j++) {
                dest_offsets[j * num_buckets + i] = v;
                v += counts[j * num_buckets + i];
            }
        },
        1 + 1024 / num_blocks);

    parallel_for(
        0, num_blocks,
        [&](size_t i) {
            size_t start = (std::min)(i * block_size, n);
            size_t end = (std::min)(start + block_size, n);
            fill_task_to_array<assignment_tag>(
                In.cut(start, end), Keys.cut(start, end),
                dest_offsets.begin() + i * num_buckets, num_buckets, Out,
                location.cut(start, end));
        },
        1, is_nested);

    return std::make_pair(std::move(bucket_offsets), false);
}

template <typename TaskIterator, typename IdFunc, typename BufferIterator,
          typename LocationIterator>
auto sort_task(slice<TaskIterator, TaskIterator> In, IdFunc g,
               slice<BufferIterator, BufferIterator> buffers,
               slice<LocationIterator, LocationIterator> location) {

    using T = typename slice<TaskIterator, TaskIterator>::value_type;
    size_t num_buckets = buffers.size();
    int key_bits = log2_up(num_buckets);

    size_t n = In.size();
    parlay::sequence<size_t> offsets;
    auto out_data = internal::uninitialized_sequence<T>(n);
    auto Out = make_slice(out_data);

    // pre sort
    size_t bits = 6;  // sort by top half bits
    size_t shift_bits = key_bits - bits;

    size_t num_outer_buckets = (size_t{1} << bits);
    size_t mask = num_outer_buckets - 1;
    auto f = [&](size_t i) {
        return static_cast<size_t>((g(In[i]) >> shift_bits) & mask);
    };

    auto get_bits = delayed_seq<size_t>(n, f);
    bool one_bucket;
    time_nested("p1", [&]() {
        std::tie(offsets, one_bucket) =
            count_sort_task<uninitialized_copy_tag, uint32_t>(
                In, Out, make_slice(get_bits), num_outer_buckets, location);
    });

    using LocationType =
        typename std::iterator_traits<LocationIterator>::value_type;
    auto location_p2 = internal::uninitialized_sequence<LocationType>(n);
    auto Location2 = make_slice(location_p2);

    auto counts_data = sequence<size_t>(num_buckets, 0);
    auto Counts = make_slice(counts_data);

    int actual_outer_buckets =
        internal::num_blocks(num_buckets, 1 << shift_bits);
    time_nested("p2", [&]() {
        parallel_for(0, actual_outer_buckets, [&](size_t i) {
            size_t start = offsets[i];
            size_t end = offsets[i + 1];
            auto sub_tasks = Out.cut(start, end);
            auto sub_locations = Location2.cut(start, end);

            size_t bucket_start = min(i << shift_bits, num_buckets);
            size_t bucket_end = min((i + 1) << shift_bits, num_buckets);
            auto sub_buffers = buffers.cut(bucket_start, bucket_end);
            auto g2 = [&](const T& t) { return g(t) - bucket_start; };
            auto sub_counts = Counts.cut(bucket_start, bucket_end);

            inner_sort_task(sub_tasks, g2, sub_buffers, sub_locations,
                            sub_counts);
        });
    });

    time_nested("location", [&]() {
        parallel_for(0, n,
                     [&](size_t i) { location[i] = Location2[location[i]]; });
    });
    return counts_data;
}

template <bool id_from_func, typename TaskIterator, typename Id,
          typename BufferIterator, typename LocationIterator,
          typename CountIterator>
void sort_task_direct(slice<TaskIterator, TaskIterator> In, Id id,
                      slice<BufferIterator, BufferIterator> buffers,
                      slice<LocationIterator, LocationIterator> location,
                      slice<CountIterator, CountIterator> counts) {
    time_nested("p1", [&]() {
        inner_sort_task<id_from_func>(In, id, buffers, location, counts);
    });
}