import matplotlib.pyplot as plt
from matplotlib.font_manager import FontManager
from pylab import mpl
import subprocess

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
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    mpl.rcParams['font.family'] = 'SimHei'
    mpl.rcParams['axes.unicode_minus'] = False
