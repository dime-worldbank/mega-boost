# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

import re
import pandas as pd

COUNTRY = 'Albania'
raw_microdata_csv_dir = prepare_raw_microdata_csv_dir(COUNTRY)

# helper function to map to regions:
def map_to_region(admin2_item):
    loc_code = admin2_item[:3]
    if loc_code in [ '032', '042', '102', '124', '139', '140', '167', '302', '303', '304',
                     '305', '306', '307', '308', '309', '310', '311', '530', '531', '745', 
                     '746', '747', '835']:
        return 'Berat'
    elif loc_code in [ '025', '043', '046', '103', '106', '132',  '315', '316', '317', '318',
                       '319', '320', '321', '345', '346', '347', '348', '349', '350', '351',
                       '352', '353', '354', '355', '356', '357', '358', '645', '646', '647',
                       '648', '649', '650', '651', '652', '653', '654', '655', '755', '756',
                       '757', '758', '759', '760', '761', '762']:
        return 'Diber'
    elif loc_code in ['016', '047', '107', '108', '118', '119', '123', '150', '151', '163', 
                      '365', '366', '367', '368', '370', '372', '521', '522', '523', '524']:
        return 'Durres'
    elif loc_code in ['048', '109', '110', '114', '128', '134', '152', '153', '381', '382', 
                      '383', '384', '385', '386', '387', '388', '389', '390', '391', '392',
                      '393', '394', '395', '396', '397', '398', '399', '400', '430', '431',
                      '432', '433', '434', '435', '436', '437', '438', '586', '587', '588',
                      '589', '590', '591', '592', '593', '594', '680', '681', '682', '683',
                      '684', '742', '743', '744', '784', '841']:
        return 'Elbasan' 
    elif loc_code in [ '024', '049', '111', '112', '113', '129', '131', '147', '410', '411',
                       '412', '413', '414', '415', '416', '417', '418', '419', '420', '421',
                       '422', '423', '485', '486', '600', '601', '602', '603', '604', '605',
                       '606', '607', '608', '609', '610', '612', '613', '614', '635', '636',
                       '637', '638', '639', '640', '641', '642', '780', '836', '837']:
        return 'Fier'
    elif loc_code in ['011', '028', '034', '115', '116', '135', '142', '143', '154', '445',
                      '446', '447', '448', '449', '450', '451', '452', '453', '454', '455',
                      '690', '691', '693', '694', '695', '697', '781', '782', '783', '786',
                      '787', '838']:
        return 'Gjirokaster'
    elif loc_code in ['014', '015', '029', '045', '105', '120', '121', '122', '136', '168',
                      '335', '336', '337', '338', '487', '488', '489', '490', '496', '497',
                      '498', '499', '500', '501', '502', '503', '504', '505', '506', '507',
                      '508', '509', '696', '705', '706', '843', '844', '845', '846']:
        return 'Korce'
    elif loc_code in ['012', '018', '036', '117', '125', '145', '460', '461', '462', '540', 
                      '541', '542', '543', '544', '545', '546', '547', '548', '549', '550',
                      '551', '552', '553', '700', '701', '702', '703', '718', '704', '719',
                      '720', '721', '722', '723', '740', '741', '820', '823', '824', '825',
                      '826']:
        return 'Kukes' 
    elif loc_code in ['020', '026', '126', '127', '133', '162', '164', '560', '562', '570',
                      '571', '572', '573', '574', '575', '576', '577', '578', '625', '666',
                      '667', '668', '669', '670']:
        return 'Lezhe' 
    elif loc_code in ['033', '130', '137', '141', '155', '157', '626', '627', '628', '629',
                      '764', '765', '766', '767', '768', '769', '822']: 
        return 'Shkoder'
    elif loc_code in ['035', '101', '165', '166', '470', '471', '472', '473', '474', '475',
                      '476', '477', '715', '716', '770', '785', '795', '796', '797', '798',
                      '799', '800', '801', '802', '803', '804', '805', '807', '808', '809',
                      '811', '812', '821']: 
        return 'Tirane'
    elif loc_code in ['037', '044', '104', '138', '146', '156', '158', '159', '160', '325',
                      '326', '328', '730', '731', '732', '734', '735', '736', '737']:
        return 'Vlore'
    return 'Central'

def pad_left(code, length = 3):
    code = str(code).split('.')[0]
    while len(code)<length:
        code = '0'+code
    return code

# COMMAND ----------

col_names = {
    'Gov Entity':'admin2',
    'Gov. Entity':'admin2',
    'Gov.Entity':'admin2',
    'Gov':'admin2',
    'ge':'admin2',
    'Line Ministri':'admin3',
    'Line Ministry':'admin3',
    'Ministry':'admin3',
    'lm':'admin3',
    'Inst.':'admin4',
    'Institution':'admin4',
    'institution': 'admin4',
    'Chapter':'fin_source',
    'ch':'fin_source',
    'program':'func3',
    'Program':'func3',
    'Progr':'func3',
    'Account':'econ5',
    'Economic Account':'econ5',
    'eccaccount':'econ5',
    'TDO':'admin5',
    'tdo':'admin5',
    'FYTD Actual':'executed',
    'actual':'executed',
    'Actual':'executed',
    'Operational Budget':'revised',
    'operationalbudget':'revised',
    'Initial Budget':'approved',
    'initialbudget':'approved',
    'Project':'project',
    'Account Description': 'Account_Description',
    'Institution Description':'Institution_Description'

}
sheet_map = {
    ('3 digit', 2023): ('Vendori', 0),
    ('3 digit', 2024): ('Sheet2', 1),
}

required_col_names = [
    'admin2', 'admin3', 'admin4', 'admin5', 'fin_source', 'func3', 'econ5', 'project', 'executed', 'revised', 'approved'
    ]

years = [2023, 2024]
for year in years:
    expense_data_files = glob(f'{RAW_INPUT_DIR}/{COUNTRY}/{year}/*.xlsx')
    for f in expense_data_files:
        if '7 digit' in f:
            df_7 = pd.read_excel(f, header=1).rename(
                columns=lambda c: col_names.get(c.strip(), c))
            df_7 = df_7.dropna(how='all')
            df_7['econ3'] = df_7['econ5'].astype(str).str[:3].astype(float)
            df_7['year'] = year
            df_7['src'] = '7 digit'
        if '3 digit' in f:
            sheet_name, header_row = sheet_map.get(('3 digit', year))
            df_3 = pd.read_excel(f, sheet_name=sheet_name, header=header_row).rename(
                columns=lambda c: col_names.get(c.strip(), c))
            df_3 = df_3.dropna(how='all')
            df_3['econ3'] = df_3['econ5'] # because 'Economic Account' is mapped to econ5 by the dict above
            df_3['year'] = year
            df_3['src'] = '3 digit'
    df = pd.concat([df_7, df_3], ignore_index=True)
    missing_cols = [col for col in required_col_names if col not in df.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"
    df['counties'] = df.admin2.map(lambda x: map_to_region(pad_left(str(x).split('.')[0], length=3)))
    outfile = f'{raw_microdata_csv_dir}/{year}.csv'
    
    df.to_csv(outfile, index=False)
