import dataset_info
import pprint

res = dataset_info.get_dataset_info('data15_13TeV:data15_13TeV.00266904.physics_Main.deriv.DAOD_MUON1.r9264_p3083_p4144_tid21196832_00')

pprint.pprint(res.to_dict('records'))
