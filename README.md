# T-Assess
Source code for T-Assess: An Efficient Data Quality Assessment System Tailored for Trajectory Data

## Dependencies
The code is tested on
- CentOS 7
- Spark 3.3.2
- Kafka 3.3.2
- Python 3.9
- numpy 1.23.3

## Run Trajectory Sampling
Supposing that trajectories are stored in the repository `./data/Tdrive/taxi`, the trajectory sampler with 50% samples and 64 grid resolution is executed in a shell command
```
python optimizer.py --data_root ./data --dataset Tdrive/taxi --ratio_samples 0.5 --num_grids 64 --save_dir ./data --seed 2023
```
