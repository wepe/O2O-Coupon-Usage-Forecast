- season one

	这个文件夹存放第一赛季的代码
	- `extract_feature.py`  划分数据集，提取特征，生成训练集（dataset1和dataset2）和预测集（dataset3）。
	- `xgb.py`  训练xgboost模型，生成特征重要性文件，生成预测结果。单模型第一赛季A榜AUC得分0.798.

- season two

	这个文件夹存放第二赛季的代码

	- `extract_feature.sql`  划分数据集，提取特征，生成训练集（dataset1和dataset2）和预测集（dataset3）。
	- `xgb.sql` 通过PAI命令调用接口训练xgboost模型，生成预测结果。单模型第二赛季A榜AUC得分0.782
	- `blending` 这个文件夹存放Blending模型的代码，包括level1的4个xgb，4个gbdt，4个rf，和level2的LR和xgb