import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("./feature_importance.csv")
df = df.sort_values("feature_importance")
frame = pd.Series(list(df['feature_importance']), index=df['colname'])
print frame

plt.figure()
frame.plot.barh()
plt.xlabel("feature importance")
plt.ylabel("feature names")
plt.gcf().set_size_inches(20, 16)
plt.gcf().set_tight_layout(True)
plt.gcf().savefig("feature_importance.png")
plt.close()