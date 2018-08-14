from libs.dataoutput.dataoutput_factory import DataOutputFactory
from libs.dataoutput.rtb_dataoutput import RTBDataOutput


class RTBDataOutputFactory(DataOutputFactory):

    def get_dataout(self,job):
        return RTBDataOutput()
