from libs.dataoutput.dataoutput_factory import DataOutputFactory
from libs.dataoutput.rtb_dataoutput import RTBDataOutput


class RTBDataOutputFactory(DataOutputFactory):

    def get_dataoutput(self,job):
        return RTBDataOutput()
