
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import QDate
from Stock.py import *
class Example(QWidget):
    
    def __init__(self):
        super().__init__()
        
        self.initUI()
        g.track=Track1()
        g.track.Init()

    def initUI(self):      

        self.resize(400,600)
        self.setWindowTitle('股票追踪')
        layout=QVBoxLayout()

        b1 = QPushButton('加载数据', self)
        #b1.move(10, 10)
        b1.clicked[bool].connect(lambda:LoadDBData())
        layout.addWidget(b1)
       
        b2 = QPushButton('日常更新', self)
        #b2.move(10, 60)
        b2.clicked[bool].connect(lambda:DailyUpdate(g.df))
        layout.addWidget(b2)

        b6 = QPushButton('更新复权', self)
        b6.clicked[bool].connect(lambda:UpdateFuQuan(g.df,'20200720','20200826'))
        layout.addWidget(b6)

        b3 = QPushButton('计算均线', self)
        #b3.move(10, 110)
        b3.clicked[bool].connect(lambda:CalcMeans(g.df))
        layout.addWidget(b3)

        b4 = QPushButton('更新追踪池', self)
        #b4.move(10, 160)
        b4.clicked[bool].connect(lambda:UpdateTrack(g.df,g.StockBasic,db))
        layout.addWidget(b4)

        b5 = QPushButton('生成报告', self)
        #b5.move(10, 200)
        b5.clicked[bool].connect(lambda:DoReport(g.trackData,g.df,self.d1.toPlainText(),g.StockBasic))
        layout.addWidget(b5)

        #b = QPushButton('更新复权', self)
        self.d1=QTextEdit()
        self.d1.setPlainText(today)
        #self.setGeometry(300, 300, 280, 170)
        layout.addWidget(self.d1)
        self.setLayout(layout)
        
        self.show()

if __name__ == '__main__':
    print('main')
    #LoadDBData()
    #adj=LoadTable("AdjFactor")
    #s=GetAdjChangeList(adj)
    app = QApplication(sys.argv)
    ex = Example()
    sys.exit(app.exec_())