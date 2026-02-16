import os
os.environ["MPLCONFIGDIR"] = r"C:\Quant\matplotlib_cache"
# use Agg to avoid any GUI requirement
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
# draw a trivial figure to force font cache creation
fig = plt.figure(figsize=(1,1))
ax = fig.add_subplot(111)
ax.plot([0,1],[0,1])
out = r"C:\Quant\matplotlib_cache\mpl_cache_test.png"
plt.savefig(out)
print("matplotlib init complete, wrote", out)
