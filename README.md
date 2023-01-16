# PIM-base

This submodule contains efficient low-level function calls we implemented, including efficient CPU-DPU communication, pipelining, performance stats collection, database correctness testing, argument parsing, random distribution generators and other miscs.

If you use our codes, please cite our paper:

[1] **PIM-tree: A Skew-resistant Index for Processing-in-Memory**. Hongbo Kang, Yiwei Zhao, Guy E. Blelloch, Laxman Dhulipala, Yan Gu, Charles McGuffey, and Phillip B. Gibbons. PVLDB, 16(4): 946-958, 2022. *doi:10.14778/3574245.3574275*. *arxiv Preprint: 2211.10516*. [[Paper](https://www.vldb.org/pvldb/volumes/16/paper/PIM-tree%3A%20A%20Skew-resistant%20Index%20for%20Processing-in-Memory)] [[arXiv](https://arxiv.org/abs/2211.10516)].

[2] **The Processing-in-Memory Model.** Hongbo Kang, Phillip B Gibbons, Guy E Blelloch, Laxman Dhulipala, Yan Gu, Charles McGuffey. 2021. In Proceedings of the 33rd ACM Symposium on Parallelism in Algorithms and Architectures. 295–306. [[doi](https://dl.acm.org/doi/10.1145/3409964.3461816)].


## Requirements

This implementation was created to facilitate the experiments in the paper. The current implementation can only run on [UPMEM](https://www.upmem.com/) machines. This codeset is built on [UPMEM SDK](https://sdk.upmem.com/).
