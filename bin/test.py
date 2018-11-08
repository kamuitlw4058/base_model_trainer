import matplotlib.pyplot as plt
from matplotlib.font_manager import FontManager
from pylab import mpl
import subprocess


for key in mpl.rcParams.keys():
    if 'font' in str(key):
        print(key)


def get_matplot_zh_font():
    fm = FontManager()
    mat_fonts = set(f.name for f in fm.ttflist)
    print(mat_fonts)
    output = str(subprocess.check_output('fc-list :lang=zh -f "%{family}\n"', shell=True))
    zh_fonts = set(f.split(',', 1)[0] for f in output.split('\n'))
    print(zh_fonts)
    available = list(mat_fonts & zh_fonts)
    #
    print('*' * 10, '可用的字体', '*' * 10)
    for f in available:
        print(f)
    available_font = [output.split('\n')[0].split(',', 1)[0]]
    print("available_font:" +  str(available_font))
    return available_font

def set_matplot_zh_font():
    #mpl.rcParams['font.sas-serig'] = ['SimHei']  # 用来正常显示中文标签
    print("123")
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    mpl.rcParams['font.family'] = 'SimHei'
    mpl.rcParams['axes.unicode_minus'] = False
    # available = get_matplot_zh_font()
    # if len(available) > 0:
    #     mpl.rcParams['font.sans-serif'] = [available[0]]    # 指定默认字体
    #     mpl.rcParams['font.family'] = 'sans-serif'
    #     mpl.rcParams['axes.unicode_minus'] = False          # 解决保存图像是负号'-'显示为方块的问题

set_matplot_zh_font()