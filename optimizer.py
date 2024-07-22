import argparse
import math
import os
import random
import numpy as np
from typing import Dict, List
import time
import pandas as pd

AIS_RANGE = [9.42, 54, 31, 66.2]
TDRIVE_RANGE = [116.08, 39.68, 116.77, 40.18]


def parse_args(args=None):
    parser = argparse.ArgumentParser(description="alias-sampling")
    parser.add_argument("--data_root", type=str, default="./data")
    parser.add_argument("--dataset", type=str, default="Tdrive/taxi")
    parser.add_argument("--num_grids", type=int, default=64)
    parser.add_argument("--ratio_samples", type=float, default=0.5)
    parser.add_argument("--save_root", type=str, default="./data")
    parser.add_argument("--seed", type=int, default=2023)
    return parser.parse_args()


def is_within_region(lon, lat, region):
    return region[0] <= lon <= region[2] and region[1] <= lat <= region[3]


class Trajectory:
    def __init__(self, traj_id, lon_vec, lat_vec, time_lst, grid_indices):
        self.traj_id = traj_id
        self.lon_vec = lon_vec
        self.lat_vec = lat_vec
        self.time_lst = time_lst
        self.grid_indices = np.array(grid_indices)

    @property
    def num_points(self):
        return len(self.lon_vec)


class TrajDataset:
    def __init__(self, traj_root, data_name, num_grid):
        self.data_root = os.path.join(traj_root, data_name)
        dataset = data_name.split('/')[0]
        self.ranges = TDRIVE_RANGE if dataset == "Tdrive" else AIS_RANGE
        self.num_grid = num_grid

    def read_data(self):
        traj_path_lst = os.listdir(self.data_root)
        traj_dataset = []
        lb_lat, ub_lat, lb_lon, ub_lon = 90, -90, 180, -180
        for traj_name in traj_path_lst:
            traj_path = os.path.join(self.data_root, traj_name)
            with open(traj_path, 'r') as t_file:
                lon_lst, lat_lst, time_lst = [], [], []
                for line in t_file.readlines():
                    content = line.strip().split(',')
                    tid = int(content[0])
                    timestamp = content[1]
                    lon, lat = float(content[2]), float(content[3])
                    if not is_within_region(lon, lat, self.ranges):
                        continue
                    lb_lat = lat if lat < lb_lat else lb_lat
                    ub_lat = lat if lat > ub_lat else ub_lat
                    lb_lon = lon if lon < lb_lon else lb_lon
                    ub_lon = lon if lon > ub_lon else ub_lon
                    lon_lst.append(lon)
                    lat_lst.append(lat)
                    time_lst.append(timestamp)
                if len(time_lst) == 0:
                    continue
                traj_dataset.append(Trajectory(tid, lon_lst, lat_lst, time_lst, None))
        print(lb_lat, ub_lat, lb_lon, ub_lon)
        lon_grid_sz = (ub_lon - lb_lon) / self.num_grid
        lat_grid_sz = (ub_lat - lb_lat) / self.num_grid
        grid_traj_dataset = []
        grid_len = 0
        for traj in traj_dataset:
            grid_lst = []
            for lon, lat in zip(traj.lon_vec, traj.lat_vec):
                if lon == ub_lon:
                    lon_grid = self.num_grid
                else:
                    lon_grid = math.floor((lon - lb_lon) / lon_grid_sz) + 1
                if lat == ub_lat:
                    lat_grid = self.num_grid
                else:
                    lat_grid = math.floor((lat - lb_lat) / lat_grid_sz) + 1
                grid_id = (lat_grid - 1) * self.num_grid + lon_grid
                grid_lst.append(grid_id)
            # print(grid_lst)
            grid_len += len(grid_lst)
            traj.grid_indices = np.array(grid_lst)
            grid_traj_dataset.append(traj)
        print("The size of the trajectory dataset: {}".format(len(grid_traj_dataset)))
        print("Avg. grid number of each trajetory: {:.4f}".format(grid_len / len(grid_traj_dataset)))
        return grid_traj_dataset, int(grid_len / len(grid_traj_dataset))


def write_samples(saved_root, samples, lineage, **kwargs):
    file_name = "samples-{}sample-{}grid-{}contain.txt".format(
        int(kwargs["ratio_samples"]),
        kwargs["num_grids"],
        kwargs["contain"]
    )
    saved_path = os.path.join(saved_root, file_name)

    with open(saved_path, 'w') as saved_file:
        saved_file.write("Number of samples: {}\tGrid resolution: {}\tContainment: {}\n"
                         .format(int(kwargs)kwargs["num_samples"], kwargs["num_grids"], kwargs["contain"]))
        saved_file.write("Cluster timing: {:.4f}sec\tAlias sample timing: {:.4f}sec\n"
                         .format(kwargs["cluster_time"], kwargs["sample_time"]))
        saved_file.write("sampled trajectory distribution:\n")
        saved_file.write(str(lineage) + "\n")
        saved_file.write("sampled trajectory id:\n")
        for traj_id in samples:
            line = "{}\n".format(traj_id)
            saved_file.write(line)


def traj_cluster(traj_dataset: List[Trajectory], containment: int):
    clusters = {}
    ccenters = []
    cluster_time = .0
    st_sort_time = time.time()
    traj_list = sorted(traj_dataset, key=lambda x: x.grid_indices.size, reverse=True)
    ed_sort_time = time.time()
    cluster_time += ed_sort_time - st_sort_time
    print("Trajectory Sorting takes {:.4f} sec.".format(ed_sort_time - st_sort_time))
    num_cluster = 0
    st_cluster_time = time.time()
    for traj in traj_list:
        flag = False
        for cid, center in enumerate(ccenters):
            overlaps = np.isin(traj.grid_indices, center).sum()
            if overlaps >= min(containment, traj.grid_indices.size):
                clusters[cid].append(traj.traj_id)
                flag = True
                break
        if not flag:
            ccenters.append(traj.grid_indices)
            clusters[num_cluster] = [traj.traj_id]
            num_cluster += 1
    ed_cluster_time = time.time()
    cluster_time += ed_cluster_time - st_cluster_time
    print("Trajectory Clustering takes {:.4f} sec.".format(ed_cluster_time - st_cluster_time))
    return clusters, cluster_time


class AliasMethod:
    def __init__(self, clusters: Dict[int, List], seed: int):
        self.clusters = clusters
        self.density = [len(trajs) for trajs in clusters.values()]
        num_cluster = len(clusters)
        sums = sum(self.density)
        self.density = [num_traj * num_cluster / sums for num_traj in self.density]
        self.alias = list(range(num_cluster))
        self.seed = seed

    def set_random_seed(self):
        random.seed(self.seed)
        np.random.seed(self.seed)

    def create_alias_table(self):
        small, large = [], []
        for cid, dens in enumerate(self.density):
            if dens > 1:
                large.append(cid)
            elif dens < 1:
                small.append(cid)
        while len(small) > 0 and len(large) > 0:
            s = small.pop()
            l = large.pop()
            self.density[l] = self.density[l] + self.density[s] - 1
            self.alias[s] = l
            if self.density[l] < 1:
                small.append(l)
            elif self.density[l] > 1:
                large.append(l)

    def sample(self, num_sample):
        traj_sample = []
        self.set_random_seed()
        sample_time = .0
        st_build_table_time = time.time()
        self.create_alias_table()
        ed_build_table_time = time.time()
        sample_time += ed_build_table_time - st_build_table_time
        print("Creating Alias Table takes {:.4f} sec.".format(ed_build_table_time - st_build_table_time))
        samples_from_clusters = {cid: 0 for cid in self.clusters.keys()}
        st_sample_time = time.time()
        while num_sample > 0:
            sample_id = np.random.randint(0, len(self.clusters))
            if np.random.rand() < self.density[sample_id]:
                if len(self.clusters[sample_id]) == 0:
                    continue
            else:
                sample_id = self.alias[sample_id]
                if len(self.clusters[sample_id]) == 0:
                    continue
            traj_sample.append(self.clusters[sample_id].pop(0))
            samples_from_clusters[sample_id] += 1
            num_sample -= 1
        ed_sample_time = time.time()
        sample_time += ed_sample_time - st_sample_time
        print("Alias Sample takes {:.4f} sec.".format(ed_sample_time - st_sample_time))
        return traj_sample, samples_from_clusters, sample_time


def write_samples(saved_root, samples, lineage, **kwargs):
    file_name = "samples-{}sample-{}grid-{}contain.txt".format(
        int(kwargs["ratio_samples"]),
        kwargs["num_grids"],
        kwargs["contain"]
    )
    saved_path = os.path.join(saved_root, file_name)

    with open(saved_path, 'w') as saved_file:
        saved_file.write("Number of samples: {}\tGrid resolution: {}\tContainment: {}\n"
                         .format(int(kwargs["ratio_samples"] * kwargs["data_size"]),
                                 kwargs["num_grids"],
                                 kwargs["contain"]))
        saved_file.write("Cluster timing: {:.4f}sec\tAlias sample timing: {:.4f}sec\n"
                         .format(kwargs["cluster_time"], kwargs["sample_time"]))
        saved_file.write("sampled trajectory distribution:\n")
        saved_file.write(str(lineage) + "\n")
        saved_file.write("sampled trajectory id:\n")
        for traj_id in samples:
            line = "{}\n".format(traj_id)
            saved_file.write(line)


def main():
    args = parse_args()
    traj_data = TrajDataset(args.data_root, args.dataset, args.num_grids)
    traj_set, avg_len = traj_data.read_data()
    
    containment = int(avg_len)
    clusters, cluster_time = traj_cluster(traj_set, containment=containment)
    cluster_size = [len(grids) for grids in clusters.values()]
    avg_size = 0
    for cid, size in enumerate(cluster_size):
        avg_size += size
        print("(id: {}, size: {})\t".format(cid, size))
    print("\nThe size of clusters: {}".format(len(clusters)))
    print("Average size of clusters: {}".format(avg_size / len(clusters)))

    num_samples = int(args.ratio_samples * len(traj_set))
    sample_operator = AliasMethod(clusters, seed=args.seed)
    sample_trajs, sample_lineage, sample_time = sample_operator.sample(num_samples)

    save_path = os.path.join(args.save_root, args.dataset.split('/')[0])
    write_samples(
        save_path, 
        sample_trajs, 
        sample_lineage,
        data_size = len(traj_set),
        ratio_samples=args.ratio_samples,
        num_grids=args.num_grids,
        contain=containment,
        cluster_time=cluster_time,
        sample_time=sample_time
    )      


if __name__ == '__main__':
    main()
