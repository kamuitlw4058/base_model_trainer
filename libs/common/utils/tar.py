import tarfile
import os



def targz(orig_dir, orig_file,tar_file):
    t = tarfile.open(tar_file , "w:gz")
    current_work_dir = os.getcwd()
    os.chdir(orig_dir)
    print(orig_file)
    t.add(orig_file)
    t.close()
    os.chdir(current_work_dir)


def untargz(tar_file,extract_file=None):
    t = tarfile.open(tar_file, "r:gz")
    if extract_file is None:
        t.extractall()
    else:
        t.extractall(path=extract_file)
    t.close()



if __name__ == "__main__":
    pass

    # from libs.conf.ftp import RESOURCES_PATH
    # from libs.utils import ftp
    # from libs.utils.tar import targz
    # from libs.utils.DatetimeUtils import get_human_timestamp
    # import os
    # ANTIFRUAD_RULE_PASS_RATIO_FILE = "anti_fraud_remit.txt"
    # ANTIFRUAD_RULE_PASS_RATIO_TARGZ_FILE = f"anti_fraud_remit_{get_human_timestamp()}.tar.gz"
    # ANTIFRUAD_RULE_PASS_RATIO_VERSION_FILE = 'anti_fraud_remit_current.txt'
    # orig_file = os.path.join(RESOURCES_PATH, ANTIFRUAD_RULE_PASS_RATIO_FILE)
    # print(orig_file)
    # targz_filename = ANTIFRUAD_RULE_PASS_RATIO_TARGZ_FILE
    # targz_filepath = os.path.join(RESOURCES_PATH, targz_filename)
    # targz(RESOURCES_PATH,ANTIFRUAD_RULE_PASS_RATIO_FILE, targz_filepath)
    # print(os.getcwd())
