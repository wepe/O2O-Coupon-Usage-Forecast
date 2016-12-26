- 代码文档
	
	`wepon`，`charles`，`tianyin` 每个文件夹存放每个队员的代码，文件夹下有简要的说明文档，代码注释较为详细，可以参考。

- 运行顺序（第二赛季）

	- 运行 `wepon/season two/extract_feature.sql` 进行数据集划分、特征提取、生成训练集和测试集。
	- 运行 `wepon/season two/xgb.sql` 通过PAI调用XGBoost进行训练，生成预测结果。
	- 运行 `tianyin`文件夹下的`java src`中的MR job和`feature`文件夹下的文件得到训练集和测试集。
	- 运行`tianyin`文件夹下的`model`文件中的PAI命令训练模型，得到GBDT的预测结果
	- 运行`charles`下的文件（运行顺序参考该文件夹下的readme文件）得到xgboost, gbdt, randomforest的预测结果
	- 运行`tianyin/ensemble`得到融合结果

- 第一赛季的只采用了单模型xgboost，`wepon/season one`
	