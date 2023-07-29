#pragma once

#include <argparse/argparse.hpp>
#include <parlay/slice.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string>
#include "fcntl.h"
#include "oracle.hpp"
#include "operation_def.hpp"
#include "timer.hpp"
#include "test_generator.hpp"
#include "operation.hpp"
using namespace std;
using namespace parlay;

void dpu_energy_stats(bool flag = false) {
    #ifdef DPU_ENERGY
        uint64_t db_iter=0, op_iter=0, cycle_iter=0, instr_iter=0;
        uint64_t op_total = 0, db_size_total = 0, cycle_total = 0;
        DPU_FOREACH(dpu_set, dpu, each_dpu) {
            DPU_ASSERT(dpu_copy_from(dpu, "op_count", 0, &op_iter, sizeof(uint64_t)));
            DPU_ASSERT(dpu_copy_from(dpu, "db_size_count", 0, &db_iter, sizeof(uint64_t)));
            DPU_ASSERT(dpu_copy_from(dpu, "cycle_count", 0, &cycle_iter, sizeof(uint64_t)));
            op_total += op_iter;
            db_size_total += db_iter;
            cycle_total += cycle_iter;
            if(flag) {
                cout<<"DPU ID: "<<each_dpu<<" "
                    <<op_iter<<" "<<db_iter<<" ";
                cout<<((op_iter > 0) ? (db_iter / op_iter) : 0)
                    <<" "<<cycle_iter<<endl;
            }
        }
        cout<<"op_total: "<<op_total<<endl;
        cout<<"db_total: "<<db_size_total<<endl;
        cout<<"cy_total: "<<cycle_total<<endl;
    #endif
}

inline void write_ops_to_file(string file_name,
                              slice<operation*, operation*> ops) {
    printf("Will write to '%s'\n", file_name.c_str());

    /* Open a file for writing.
     *  - Creating the file if it doesn't exist.
     *  - Truncating it to 0 size if it already exists. (not really needed)
     *
     * Note: "O_WRONLY" mode is not sufficient when mmaping.
     */

    const char* filepath = file_name.c_str();

    int try_to_unlink = unlink(filepath);

    if (try_to_unlink == 0) {
        cout << "Remove: " << filepath << endl;
    }

    int fd = open(filepath, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);

    if (fd == -1) {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }

    // Stretch the file size to the size of the (mmapped) array of char

    int n = ops.size();
    size_t filesize = sizeof(operation) * n;

    if (lseek(fd, filesize - sizeof(operation), SEEK_SET) == -1) {
        close(fd);
        perror("Error calling lseek() to 'stretch' the file");
        exit(EXIT_FAILURE);
    }

    operation empty_op;
    empty_op.type = operation_t::empty_t;

    if (write(fd, &empty_op, sizeof(operation)) == -1) {
        close(fd);
        perror("Error writing last byte of the file");
        exit(EXIT_FAILURE);
    }

    // Now the file is ready to be mmapped.
    void* map = mmap(0, filesize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    operation* fops = (operation*)map;

    parlay::parallel_for(0, n, [&](size_t i) { fops[i] = ops[i]; });

    cout << "task generation finished" << endl;

    // Write it now to disk
    if (msync(map, filesize, MS_SYNC) == -1) {
        perror("Could not sync the file to disk");
    }

    // Don't forget to free the mmapped memory
    if (munmap(map, filesize) == -1) {
        close(fd);
        perror("Error un-mmapping the file");
        exit(EXIT_FAILURE);
    }

    // Un-mmaping doesn't close the file, so we still need to do that.
    close(fd);
}

template <typename Checker>
auto read_op_file(string name, Checker checker) {
    const char* filepath = name.c_str();

    int fd = open(filepath, O_RDONLY, (mode_t)0600);

    if (fd == -1) {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }

    struct stat fileInfo;

    if (fstat(fd, &fileInfo) == -1) {
        perror("Error getting the file size");
        exit(EXIT_FAILURE);
    }

    if (fileInfo.st_size == 0) {
        fprintf(stderr, "Error: File is empty, nothing to do\n");
        exit(EXIT_FAILURE);
    }

    printf("File size is %ji\n", (intmax_t)fileInfo.st_size);

    void* map = mmap(0, fileInfo.st_size, PROT_READ, MAP_SHARED, fd, 0);

    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    cout << fileInfo.st_size << ' ' << sizeof(operation) << endl;

    assert(fileInfo.st_size % sizeof(operation) == 0);

    int n = fileInfo.st_size / sizeof(operation);
    auto operation_map = (operation*)map;
    auto operations = parlay::tabulate(n, [&](size_t i) {
        operation& op = operation_map[i];
        checker(op);
        return op;
    });

    return operations;
}

class core {
   public:
    static batch_parallel_oracle oracle;
    static bool check_result;
    static int batch_number;

    // core(bool cr) {
    //     check_result = cr;
    // }

    static void get(slice<get_operation*, get_operation*> ops) {
        cout << (batch_number++) << " " << __FUNCTION__ << endl;
        auto ops2 = make_slice((int64_t*)ops.begin(), (int64_t*)ops.end());
        time_start("get");
        auto v1 = pim_skip_list::get(ops2);
        time_end("get");
        if (check_result) {
            auto oracle_kv = oracle.predecessor_batch(ops2);
            auto v2 = oracle_kv;
            // auto v2 = parlay::map(oracle_kv,
            //                       [](const key_value& kv) { return kv.value; });
            if (!parlay::equal(v1, v2)) {
                int n = v1.size();
                for (int i = 0; i < n; i++) {
                    if (v1[i] != v2[i]) {
                        printf("[%8d]\t", i);
                        cout << "k=" << ops[i].key << "\tv1=" << v1[i]
                             << "\tv2=" << v2[i] << endl;
                    }
                }
            }
        }
    }

    static void update(slice<update_operation*, update_operation*> ops) {
        cout << (batch_number++) << " " << __FUNCTION__ << endl;
        time_start("update");
        auto ops2 = make_slice((key_value*)ops.begin(), (key_value*)ops.end());
        time_end("update");
        pim_skip_list::update(ops2);
        if (check_result) {
            oracle.insert_batch(ops2);
        }
    }

    // Range Scan
    static void scan(slice<scan_operation*, scan_operation*> ops, int64_t expected_length = 100) {
        cout << (batch_number++) << " " << __FUNCTION__ << " " << ops.size() << endl;
        int64_t range_size = INT64_MAX / oracle.inserted.size() * 2 * expected_length;
        parfor_wrap(0, ops.size(), [&](size_t i){
            if(ops[i].lkey < INT64_MAX - range_size)
                ops[i].rkey = ops[i].lkey + range_size;
        });
        int64_t max_batch_size = BATCH_SIZE / 2 / expected_length;
        slice<scan_operation*, scan_operation*> ops2 = ops;
        parlay::sequence<scan_operation> ops_tmp;
        if(check_result && ops.size() > max_batch_size){
            ops_tmp = parlay::tabulate(max_batch_size, [&](size_t i){return ops[i];});
            ops2 = parlay::make_slice(ops_tmp);
            cout<<"Batch size -> "<<max_batch_size<<" to avoid overflow."<<endl;
        }
        time_start("scan");
        auto v1 = pim_skip_list::scan(ops2);
        time_end("scan");
        int64_t length = ops2.size();
        if(check_result) {
            auto v2 = oracle.scan_size_batch(ops2);
            bool correct = true;
            parlay::parallel_for(0, length, [&](size_t i){
                if(correct) {
                    int64_t v1l = (v1.second[i].second - v1.second[i].first);
                    int64_t v2l = (v2[i].second - v2[i].first);
                    if(v1l - v2l > 1 || v2l - v1l > 1) {
                        correct = false;
                    }
                }
            });
            if(!correct) {
                int64_t v1l, v2l, res = 0;
                for(int64_t i = 0; i < length; i++) {
                    v1l = v1.second[i].second - v1.second[i].first;
                    v2l = v2[i].second - v2[i].first;
                    if(v1l - v2l > 1 || v2l - v1l > 1) {
                        if(res < 20)
                            printf("k=(%lld,%lld) v1_s=%lld v2_s=%lld i=%lld\n",
                                ops2[i].lkey, ops2[i].rkey, v1l, v2l, i);
                        res++;
                    }
                }
                cout<<"Number of Errorness: "<<res<<endl;
                cout<<"kv_set size returned: "<<v1.first.size()<<endl;
            }
        }
    }

    static void predecessor(
        slice<predecessor_operation*, predecessor_operation*> ops) {
        cout << (batch_number++) << " " << __FUNCTION__ << " " << ops.size() << endl;
        auto ops2 = make_slice((int64_t*)ops.begin(), (int64_t*)ops.end());
        time_start("predecessor");
        auto v1 = pim_skip_list::predecessor(ops2);
        time_end("predecessor");
        if (check_result) {
            auto v2 = oracle.predecessor_batch(ops2);
            if (!parlay::equal(v1, v2)) {
                int n = v1.size();
                for (int i = 0; i < n; i++) {
                    if (v1[i] != v2[i]) {
                        printf("[%8d]\t", i);
                        cout << "k=" << ops[i].key << "\tv1=" << v1[i]
                             << "\tv2=" << v2[i] << endl;
                    }
                }
            }
        }
    }

    static void insert(slice<insert_operation*, insert_operation*> ops) {
        cout << (batch_number++) << " " << __FUNCTION__ << " " << ops.size() << endl;
        auto ops2 = make_slice((key_value*)ops.begin(), (key_value*)ops.end());
        time_start("insert");
        pim_skip_list::insert(ops2);
        time_end("insert");
        if (check_result) {
            oracle.insert_batch(ops2);
        }
    }

    static void remove(slice<remove_operation*, remove_operation*> ops) {
        cout << (batch_number++) << " " << __FUNCTION__ << " " << ops.size() << endl;
        auto ops2 = make_slice((int64_t*)ops.begin(), (int64_t*)ops.end());
        time_start("remove");
        pim_skip_list::remove(ops2);
        time_end("remove");
        if (check_result) {
            oracle.remove_batch(ops2);
        }
    }

    static void execute(parlay::slice<operation*, operation*> ops,
                        int load_batch_size, int execute_batch_size) {
        printf("execute n=%lu batchsize=%d,%d\n", ops.size(), load_batch_size,
               execute_batch_size);
        memset(op_count, 0, sizeof(op_count));
        time_nested("global_exec", [&]() {
            int _block_size = 1000;
            int l = parlay::internal::num_blocks(load_batch_size, _block_size);
            parlay::sequence<size_t>* sums =
                new parlay::sequence<size_t>[OPERATION_NR_ITEMS];
            int cnts[OPERATION_NR_ITEMS];
            for (int i = 0; i < OPERATION_NR_ITEMS; i++) {
                sums[i] = parlay::sequence<size_t>(l);
                cnts[i] = 0;
            }

            int n = ops.size();
            int rounds = parlay::internal::num_blocks(n, load_batch_size);

            for (int T = 0; T < rounds; T++) {
                int l = T * load_batch_size;
                int r = min((T + 1) * load_batch_size, n);
                int len = r - l;

                auto mixed_op_batch = ops.cut(l, r);

                parlay::internal::sliced_for(
                    len, _block_size, [&](size_t i, size_t s, size_t e) {
                        size_t c[OPERATION_NR_ITEMS] = {0};
                        for (size_t j = s; j < e; j++) {
                            int t = mixed_op_batch[j].type;
                            assert(j < (size_t)len);
                            assert(t >= 0 && t < OPERATION_NR_ITEMS);
                            c[t]++;
                        }
                        for (int j = 0; j < OPERATION_NR_ITEMS; j++) {
                            sums[j][i] = c[j];
                        }
                    });
                for (int j = 0; j < OPERATION_NR_ITEMS; j++) {
                    cnts[j] = parlay::scan_inplace(parlay::make_slice(sums[j]),
                                                   parlay::addm<size_t>());
                }
                parlay::internal::sliced_for(
                    len, _block_size, [&](size_t i, size_t s, size_t e) {
                        size_t c[OPERATION_NR_ITEMS];
                        for (int j = 0; j < OPERATION_NR_ITEMS; j++) {
                            c[j] = sums[j][i] + op_count[j];
                        }
                        for (size_t j = s; j < e; j++) {
                            operation_t& operation_type =
                                mixed_op_batch[j].type;
                            int x = (int)operation_type;
                            operation& t = mixed_op_batch[j];
                            switch (operation_type) {
                                case operation_t::get_t: {
                                    get_ops[c[x]++] = t.tsk.g;
                                    break;
                                }
                                case operation_t::update_t: {
                                    update_ops[c[x]++] = t.tsk.u;
                                    break;
                                }
                                case operation_t::predecessor_t: {
                                    predecessor_ops[c[x]++] = t.tsk.p;
                                    break;
                                }
                                case operation_t::scan_t: {
                                    scan_ops[c[x]++] = t.tsk.s;
                                    break;
                                }
                                case operation_t::insert_t: {
                                    insert_ops[c[x]++] = t.tsk.i;
                                    break;
                                }
                                case operation_t::remove_t: {
                                    remove_ops[c[x]++] = t.tsk.r;
                                    break;
                                }
                                default: {
                                    assert(false);
                                }
                            }
                        }
                    });

                for (int j = 0; j < OPERATION_NR_ITEMS; j++) {
                    op_count[j] += cnts[j];
                }
                for (int j = 0; j < OPERATION_NR_ITEMS; j++) {
                    if (op_count[j] >= execute_batch_size) {
                        switch (j) {
                            case (int)operation_t::get_t: {
                                core::get(parlay::make_slice(
                                    get_ops, get_ops + op_count[j]));
                                break;
                            }
                            case (int)operation_t::update_t: {
                                assert(false);
                                break;
                            }
                            case (int)operation_t::predecessor_t: {
                                core::predecessor(parlay::make_slice(
                                    predecessor_ops,
                                    predecessor_ops + op_count[j]));
                                break;
                            }
                            case (int)operation_t::scan_t: {
                                int scan_batch_size = execute_batch_size / 100;
                                int ii = 0;
                                for(ii = 0; ii < op_count[j] - scan_batch_size; ii += scan_batch_size){
                                    core::scan(parlay::make_slice(
                                        scan_ops + ii, scan_ops + ii + scan_batch_size));
                                }
                                core::scan(parlay::make_slice(
                                    scan_ops + ii, scan_ops + op_count[j]));
                                break;
                            }
                            case (int)operation_t::insert_t: {
                                core::insert(parlay::make_slice(
                                    insert_ops, insert_ops + op_count[j]));
                                break;
                            }
                            case (int)operation_t::remove_t: {
                                core::remove(parlay::make_slice(
                                    remove_ops, remove_ops + op_count[j]));
                                break;
                            }
                        }
                        op_count[j] = 0;
                    }
                }
            }
            
            // finish remaining parts
            {
                for (int j = 0; j < OPERATION_NR_ITEMS; j++) {
                    if (op_count[j] >= 1) {
                        switch (j) {
                            case (int)operation_t::get_t: {
                                core::get(parlay::make_slice(
                                    get_ops, get_ops + op_count[j]));
                                break;
                            }
                            case (int)operation_t::update_t: {
                                assert(false);
                                break;
                            }
                            case (int)operation_t::predecessor_t: {
                                core::predecessor(parlay::make_slice(
                                    predecessor_ops,
                                    predecessor_ops + op_count[j]));
                                break;
                            }
                            case (int)operation_t::scan_t: {
                                int scan_batch_size = execute_batch_size / 100;
                                int ii = 0;
                                for(ii = 0; ii < op_count[j] - scan_batch_size; ii += scan_batch_size){
                                    core::scan(parlay::make_slice(
                                        scan_ops + ii, scan_ops + ii + scan_batch_size));
                                }
                                core::scan(parlay::make_slice(
                                    scan_ops + ii, scan_ops + op_count[j]));
                                break;
                            }
                            case (int)operation_t::insert_t: {
                                core::insert(parlay::make_slice(
                                    insert_ops, insert_ops + op_count[j]));
                                break;
                            }
                            case (int)operation_t::remove_t: {
                                core::remove(parlay::make_slice(
                                    remove_ops, remove_ops + op_count[j]));
                                break;
                            }
                        }
                        op_count[j] = 0;
                    }
                }
            }
        });
        printf("execute finish!\n");
        fflush(stdout);
        cout << oracle.inserted.size() << endl;
    }
};

batch_parallel_oracle core::oracle;
bool core::check_result = true;
int core::batch_number = 0;

class frontend {
   public:
    virtual sequence<operation> init_tasks() = 0;
    virtual sequence<operation> test_tasks() = 0;
};

class frontend_by_file : public frontend {
   public:
    string init_file;
    string test_file;
    int init_n;
    int test_n;

    frontend_by_file(string _if, string _tf, int _in = -1, int _tn = -1) {
        init_file = _if;
        test_file = _tf;
        init_n = _in;
        test_n = _tn;
    }

    sequence<operation> init_tasks() {
        auto ops = read_op_file(init_file, [&](const operation& op) {
            assert(op.type == insert_t);
        });
        if (init_n == -1) {
            return ops;
        } else {
            return tabulate(init_n, [&](size_t i) {
                return ops[i];
            });
        }
        // return ops;
    }

    sequence<operation> test_tasks() {
        auto ops = read_op_file(test_file, [&](const operation& op) {
            assert(op.type != empty_t);
        });
        if (test_n == -1) {
            return ops;
        } else {
            return tabulate(test_n, [&](size_t i) {
                return ops[i];
            });
        }
        // return ops;
    }
};

class frontend_by_generation : public frontend {
   public:
    int init_n, test_n;
    sequence<double> pos;
    int bias;
    batch_parallel_oracle oracle;
    int execute_batch_size;

    frontend_by_generation(int _init_n, int _test_n, sequence<double> _pos,
                           int _bias, int _size)
        : init_n{_init_n},
          test_n{_test_n},
          pos{_pos},
          bias{_bias},
          execute_batch_size{_size} {}

    sequence<operation> init_tasks() {
        sequence<double> init_pos = sequence<double>(OPERATION_NR_ITEMS, 0);
        init_pos[operation_t::insert_t] = 1.0;
        test_generator tg(make_slice(init_pos), execute_batch_size);
        auto ops = sequence<operation>(init_n);
        tg.fill_with_random_ops(make_slice(ops));
        auto kvs = parlay::delayed_seq<key_value>(ops.size(), [&](size_t i) {
            return (key_value){.key = ops[i].tsk.i.key,
                               .value = ops[i].tsk.i.value};
        });
        oracle.init(make_slice(kvs));
        return ops;
    }

    sequence<operation> test_tasks() {
        assert(pos.size() == OPERATION_NR_ITEMS);
        test_generator tg(make_slice(this->pos), execute_batch_size);
        auto ops = sequence<operation>(test_n);
        // auto keys = parlay::delayed_seq<int64_t>(
        //     oracle.inserted.size(),
        //     [&](size_t i) { return oracle.inserted[i].key; });
        tg.fill_with_biased_ops(make_slice(ops), false, 0.0, bias, oracle,
                                execute_batch_size);
        return ops;
    }
};

class frontend_testgen {
   public:
    int init_n, test_n;
    sequence<double> pos;
    int bias;
    string init_file;
    string test_file;
    batch_parallel_oracle oracle;
    int execute_batch_size;

    frontend_testgen(int _init_n, int _test_n, sequence<double> _pos, int _bias,
                     string initfile, string testfile, int batch_size)
        : init_n{_init_n},
          test_n{_test_n},
          pos{_pos},
          bias{_bias},
          init_file{initfile},
          test_file{testfile},
          execute_batch_size{batch_size} {}

    sequence<operation> generate_tasks(parlay::sequence<double>& possi, int n,
                                       bool zipf, double alpha, int bias) {
        assert(pos.size() == OPERATION_NR_ITEMS);
        test_generator tg(make_slice(possi), execute_batch_size);
        auto ops = sequence<operation>(n);
        tg.fill_with_biased_ops(make_slice(ops), zipf, alpha, bias, oracle,
                                execute_batch_size);
        return ops;
    }

    void init_oracle(slice<operation*, operation*> ops) {
        auto kvs = parlay::delayed_seq<key_value>(ops.size(), [&](size_t i) {
            return (key_value){.key = ops[i].tsk.i.key,
                               .value = ops[i].tsk.i.value};
        });
        oracle.init(make_slice(kvs));
    }

    void write_init_file(string init_file_name) {
        auto init_pos = sequence<double>(OPERATION_NR_ITEMS, 0);
        init_pos[operation_t::insert_t] = 1.0;
        auto ops = generate_tasks(init_pos, init_n, false, 0.0, 1);
        init_oracle(make_slice(ops));
        write_ops_to_file(init_file_name, make_slice(ops));
        auto ops_sorted =
            parlay::sort(ops, [](const operation& a, const operation& b) {
                return a.tsk.i.key < b.tsk.i.key;
            });
        write_ops_to_file(init_file_name + "sorted", make_slice(ops_sorted));
    }

    void write_file() {
        { write_init_file(this->init_file); }
        {
            auto test_ops =
                generate_tasks(this->pos, test_n, false, 0.0, this->bias);
            write_ops_to_file(test_file, make_slice(test_ops));
        }
    }

    void generate_all_test() {
        if (this->init_file.length() == 0) {
            write_init_file("/scratch/hongbo/init.in");
        } else {
            auto ops = read_op_file(this->init_file, [&](const operation& op) {
                assert(op.type == insert_t);
            });
            init_oracle(make_slice(ops));

            auto ops_sorted =
                parlay::sort(ops, [](const operation& a, const operation& b) {
                    return a.tsk.i.key < b.tsk.i.key;
                });
            write_ops_to_file("/scratch/hongbo/init_sorted.in",
                              make_slice(ops_sorted));
        }
        cout << "finish init" << endl;

        for (int t = 1; t < OPERATION_NR_ITEMS; t++) {
            if (t == 2 || t == 4 || t == 6) continue;
            auto pos = sequence<double>(OPERATION_NR_ITEMS, 0);
            pos[t] = 1.0;
            for (double alpha = 0.0; alpha <= 1.0; alpha += 0.2) {
                alpha = 0.99;
                auto ops = generate_tasks(pos, test_n, true, alpha, 1.0);
                stringstream ss;
                ss << "/scratch/hongbo/test_" << test_n << "_" << alpha << "_"
                   << t << ".in";
                string filename;
                ss >> filename;
                write_ops_to_file(filename, make_slice(ops));
                cout << filename << endl;
                if (alpha == 0.99) {
                    break;
                }
            }
        }
        exit(0);
    }
};

class driver {
   public:
    static argparse::ArgumentParser parser() {
        argparse::ArgumentParser program;
        program.add_argument("--file", "-f")
            .help("--file [init_file] [test_file]")
            .default_value(vector<string>())
            .nargs(2);
        program.add_argument("--nocheck", "-c")
            .help("stop checking the correctness of the tested data structure")
            .default_value(false)
            .implicit_value(true);
        program.add_argument("--noprint", "-t")
            .help("don't print timer name when timing")
            .default_value(false)
            .implicit_value(true);
        program.add_argument("--nodetail", "-d")
            .help("don't show detail")
            .default_value(false)
            .implicit_value(true);
        program.add_argument("--get", "-g")
            .help("-g [get_ratio]")
            .default_value(0.0)
            .scan<'g', double>();
        program.add_argument("--update", "-u")
            .help("-u [update_ratio]")
            .default_value(0.0)
            .scan<'g', double>();
        program.add_argument("--predecessor", "-p")
            .help("-p [predecessor_ratio]")
            .default_value(1.0)
            .scan<'g', double>();
        program.add_argument("--scan", "-s")
            .help("-s [scan_ratio]")
            .default_value(0.0)
            .scan<'g', double>();
        program.add_argument("--insert", "-i")
            .help("-i [insert_ratio]")
            .default_value(0.0)
            .scan<'g', double>();
        program.add_argument("--remove", "-r")
            .help("-r [remove_ratio]")
            .default_value(0.0)
            .scan<'g', double>();
        program.add_argument("--length", "-l")
            .help("-l [init_length] [test_length]")
            .nargs(2)
            .default_value(vector<int>{80000000, 20000000})
            .scan<'i', int>();
        program.add_argument("--batch", "-b")
            .help("-b [batch_size]")
            .default_value(1000000)
            .scan<'i', int>();
        program.add_argument("--bias")
            .help("--bias [?x]")
            .default_value(1)
            .scan<'i', int>();
        program.add_argument("--alpha")
            .help("--alpha [?x]")
            .default_value(0.0)
            .scan<'g', double>();
        program.add_argument("--output", "-o")
            .help("--output [init_file] [test_file]")
            .default_value(vector<string>())
            .nargs(2);
        program.add_argument("--generate_all_test_cases")
            .help("generate all test cases [initfile]")
            .default_value(string(""));
        program.add_argument("--init_state")
            .help("init state")
            .default_value(false)
            .implicit_value(true);

        return program;
    }

    // static void test_by_generator(parlay::sequence<double> pos, int init_n,
    //                               int test_n, int batch_size) {
    //     frontend_by_generation t(move(pos));
    //     t.init(init_n, batch_size);
    //     t.run(test_n, batch_size);
    // }

    static void init() {
        rn_gen::init();
        init_io_managers();
    }

    static void run(frontend& f, int batch_size) {
        pim_skip_list::init();
        // pim_skip_list::init_state = true;
        {
            auto init_ops = f.init_tasks();
            core::execute(make_slice(init_ops), batch_size, batch_size);
        }
        pim_skip_list::init_state = false;
        total_communication = 0;
        total_actual_communication = 0;
        reset_all_timers();
        {
            auto test_ops = f.test_tasks();

            dpu_energy_stats(false);
            #ifdef USE_PAPI
            papi_init_program(parlay::num_workers());
            papi_reset_counters();
            papi_turn_counters(true);
            papi_check_counters(parlay::worker_id());
            papi_wait_counters(true, parlay::num_workers());
            #endif

            core::execute(make_slice(test_ops), batch_size, batch_size);

            #ifdef USE_PAPI
            papi_turn_counters(false);
            papi_check_counters(parlay::worker_id());
            papi_wait_counters(false, parlay::num_workers());
            #endif

            print_all_timers(timer::print_type::pt_full);
            print_all_timers(timer::print_type::pt_time);
            print_all_timers(timer::print_type::pt_name);

            dpu_energy_stats(false);
          
            #ifdef USE_PAPI
            papi_print_counters(1);
            #endif
        }
    }

    static void exec(int argc, char* argv[]) {
        auto program = parser();

        try {
            program.parse_args(argc, argv);
        } catch (const std::runtime_error& err) {
            std::cerr << err.what() << std::endl;
            std::cerr << program;
            std::exit(1);
        }

        parlay::sequence<double> pos(OPERATION_NR_ITEMS, 0.0);
        pos[1] = program.get<double>("-g");
        pos[2] = program.get<double>("-u");
        pos[3] = program.get<double>("-p");
        pos[4] = program.get<double>("-s");
        pos[5] = program.get<double>("-i");
        pos[6] = program.get<double>("-r");
        core::check_result = (program["--nocheck"] == false);
        timer::print_when_time = (program["--noprint"] == false);
        timer::default_detail = (program["--nodetail"] == false);
        int bias = program.get<int>("--bias");
        double alpha = program.get<double>("--alpha");
        auto files = program.get<vector<string>>("-f");
        auto output_file = program.get<vector<string>>("-o");
        auto ns = program.get<vector<int>>("-l");
        assert(ns.size() == 2);
        int init_n = ns[0];
        int test_n = ns[1];
        int batch_size = program.get<int>("-b");
        pim_skip_list::init_state = (program["--init_state"] == true);
        // bool generate_all_testcases =
        //     (program["--generate_all_test_cases"] == true);

        if (program.is_used("--generate_all_test_cases") == true) {
            printf("To generated file:\n");
            for (int i = 1; i < OPERATION_NR_ITEMS; i++) {
                printf("pos[%d]=%lf\n", i, pos[i]);
            }
            printf("\n");
            cout << "start generating all tests" << endl;
            string init_file = program.get<string>("--generate_all_test_cases");
            frontend_testgen frontend(init_n, test_n, move(pos), bias,
                                      init_file, "", batch_size);
            frontend.generate_all_test();
        } else if (files.size() > 0) {  // test from file
            assert(files.size() == 2);
            printf("Test from file: [%s] [%s]\n", files[0].c_str(),
                   files[1].c_str());
            int in = -1, tn = -1;
            if (program.is_used("-l")) {
                in = ns[0];
                tn = ns[1];
            }
            frontend_by_file frontend(files[0], files[1], in, tn);
            run(frontend, batch_size);
        } else if (output_file.size() > 0) {  // print test file
            assert(output_file.size() == 2);
            printf("To generated file:\n");
            for (int i = 1; i < OPERATION_NR_ITEMS; i++) {
                printf("pos[%d]=%lf\n", i, pos[i]);
            }
            printf("\n");

            frontend_testgen frontend(init_n, test_n, move(pos), bias,
                                      output_file[0], output_file[1],
                                      batch_size);
            frontend.write_file();
        } else {  // in memory test
            printf("Test with generated data:\n");
            for (int i = 1; i < OPERATION_NR_ITEMS; i++) {
                printf("pos[%d]=%lf\n", i, pos[i]);
            }
            printf("\n");

            auto ns = program.get<vector<int>>("-l");
            assert(ns.size() == 2);
            int init_n = ns[0];
            int test_n = ns[1];
            frontend_by_generation frontend(init_n, test_n, move(pos), bias,
                                            batch_size);
            run(frontend, batch_size);
        }

        cout << "total communication " << total_communication << endl;
        cout << "total actual communication " << total_actual_communication
             << endl;
    }
};
