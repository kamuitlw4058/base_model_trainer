from libs.dataoutput.dataoutput_factory import DataOutputFactory
from libs.dataoutput.rtb_dataoutput import RTBDataOutput


class RTBDataOutputFactory(DataOutputFactory):
    def __init__(self):
        self._output = RTBDataOutput()

    def get_dataoutput(self):
        return self._output
