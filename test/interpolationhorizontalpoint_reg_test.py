
# -*- coding: utf-8 -*-
from fstpy import STATIONSFB
from test import TEST_PATH, TMP_PATH

import pytest
from fstpy.dataframe_utils import fstcomp
from fstpy.interpolationhorizontalpoint import InterpolationHorizontalPoint
from fstpy.std_reader import StandardFileReader
from fstpy.std_writer import StandardFileWriter
from fstpy.utils import delete_file
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
import datetime

pytestmark = [pytest.mark.interpolation_regtests] #, pytest.mark.regressions

# ::::::::::::::
# inputFile.csv
# ::::::::::::::
# pds:LAT,LATLON
# level:1.0
# 45.73,43.40,49.18

# pds:LON,LATLON
# level:1.0
# -73.75,-79.38,-123.18
@pytest.fixture
def date_of_origin():
    d = datetime.datetime(year=2015, month=8, day=5,hour=9,minute=42,second=30)
    return d

@pytest.fixture
def simple_input_df(date_of_origin):
    latlon = [
        {'nomvar':'LAT','etiket':'LATLON','level':'1.0','d':np.array([45.73,43.40,49.18],dtype='float32'),'date_of_origin':date_of_origin},
        {'nomvar':'LON','etiket':'LATLON','level':'1.0','d':np.array([-73.75,-79.38,-123.18],dtype='float32'),'date_of_origin':date_of_origin}
        ]
    return pd.DataFrame(latlon)    


# ::::::::::::::
# latlonExtrapolation_fileSrc.csv
# ::::::::::::::
# pds:LAT,LATLON
# level:1.0
# 43.86,-43.4,43.61,43.47,43.22,-44.0

# pds:LON,LATLON
# level:1.0
# -78.926,77.7,-78.380,-79.26,-78.72,70.0
@pytest.fixture
def latlon_extrapolation_df(date_of_origin):
    latlon = [
        {'nomvar':'LAT','etiket':'LATLON','level':'1.0','d':np.array([43.86,-43.4,43.61,43.47,43.22,-44.0],dtype='float32'),'date_of_origin':date_of_origin},
        {'nomvar':'LON','etiket':'LATLON','level':'1.0','d':np.array([-78.926,77.7,-78.380,-79.26,-78.72,70.0],dtype='float32'),'date_of_origin':date_of_origin},
        ]
    return pd.DataFrame(latlon)   

# ::::::::::::::
# latlonWithGrid_fileSrc.csv
# ::::::::::::::
# gds:TYPE_L,1,2,3

# pds:LAT,LATLON
# level:1.0
# 45.73,43.40,49.18

# pds:LON,LATLON
# level:1.0
# -73.75,-79.38,-123.18
@pytest.fixture
def latlon_with_grid_df(date_of_origin):
    latlon = [
        {'nomvar':'LAT','etiket':'LATLON','level':'1.0','grtyp':'L','d':np.array([45.73,43.40,49.18],dtype='float32'),'date_of_origin':date_of_origin},
        {'nomvar':'LON','etiket':'LATLON','level':'1.0','grtyp':'L','d':np.array([-73.75,-79.38,-123.18],dtype='float32'),'date_of_origin':date_of_origin},
        ]
    return pd.DataFrame(latlon)   

# ::::::::::::::
# latlonYY_fileSrc.csv
# ::::::::::::::
# pds:LAT,LATLON
# level:1000.0
# 46.60,14.098,-45.828,-13.458,51.048,-8.49,56.056,-43.81,-4.7,-51.8,-80.11,-14.034,-15.68,34.63,36.22,76.28

# pds:LON,LATLON
# level:1000.0
# -67.368,-44.74,-33.34,38.155,31.50,116.93,158.32,170.64,-52.73,-59.236,100.28,127.28,40.56,-140.24,-30.495,-99.63
@pytest.fixture
def latlon_yy_df(date_of_origin):
    latlon = [
        {'nomvar':'LAT','etiket':'LATLON','level':'1000.0','d':np.array([46.60,14.098,-45.828,-13.458,51.048,-8.49,56.056,-43.81,-4.7,-51.8,-80.11,-14.034,-15.68,34.63,36.22,76.28],dtype='float32'),'date_of_origin':date_of_origin},
        {'nomvar':'LON','etiket':'LATLON','level':'1000.0','d':np.array([-67.368,-44.74,-33.34,38.155,31.50,116.93,158.32,170.64,-52.73,-59.236,100.28,127.28,40.56,-140.24,-30.495,-99.63],dtype='float32'),'date_of_origin':date_of_origin},
        ]
    return pd.DataFrame(latlon)   
# ::::::::::::::
# latlon_fileSrc.csv
# ::::::::::::::
# pds:LAT,LATLON
# level:1.0
# 45.73,43.40,49.18

# pds:LON,LATLON
# level:1.0
# -73.75,-79.38,-123.18
@pytest.fixture
def latlon_df(date_of_origin):
    latlon = [
        {'nomvar':'LAT','etiket':'LATLON','level':'1.0','d':np.array([45.73,43.40,49.18],dtype='float32'),'date_of_origin':date_of_origin},
        {'nomvar':'LON','etiket':'LATLON','level':'1.0','d':np.array([-73.75,-79.38,-123.18],dtype='float32'),'date_of_origin':date_of_origin},
        ]
    return pd.DataFrame(latlon)   

# ::::::::::::::
# latlon_fileSrc2.csv
# ::::::::::::::
# pds:LAT,LATLON
# level:1.0
# 45.73,43.40,49.18,53.13

# pds:LON,LATLON
# level:1.0
# -73.75,-79.38,-123.18,-108.15
@pytest.fixture
def latlon2_df(date_of_origin):
    latlon = [
        {'nomvar':'LAT','etiket':'LATLON','level':'1.0','d':np.array([45.73,43.40,49.18,53.13],dtype='float32'),'date_of_origin':date_of_origin},
        {'nomvar':'LON','etiket':'LATLON','level':'1.0','d':np.array([-73.75,-79.38,-123.18,-108.15],dtype='float32'),'date_of_origin':date_of_origin},
        ]
    return pd.DataFrame(latlon)   

@pytest.fixture
def stationsdf_df(date_of_origin):
    latlon = [
        {'nomvar':'LAT','etiket':'LATLON','level':'1.0','d':STATIONSFB['Latitude'].to_numpy().astype('float32'),'date_of_origin':date_of_origin},
        {'nomvar':'LON','etiket':'LATLON','level':'1.0','d':STATIONSFB['Longitude'].to_numpy().astype('float32'),'date_of_origin':date_of_origin},
        ]
    return pd.DataFrame(latlon)  

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"InterpolationHorizontalPoint/testsFiles/"


def test_regtest_1(plugin_test_dir,latlon_df):
    """Test #1 : test_onlyscalarR1Operational"""
    # open and read source
    source0 = plugin_test_dir + "4panneaux_input4_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_df).compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_1.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resultOnlyScalarR1Operational_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_1"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_2(plugin_test_dir,latlon_df):
    """Test #2 : test_onlyscalar"""
    # open and read source
    source0 = plugin_test_dir + "4panneaux_input4_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_df).compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_2.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resultOnlyScalar_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_2"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_3(plugin_test_dir,latlon_df):
    """Test #3 : test_scalarvectorial"""
    # open and read source
    source0 = plugin_test_dir + "tape10.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_df).compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_3.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resultScalarVectorial_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_2"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_4(plugin_test_dir,latlon2_df):
    """Test #4 : test_scalarvectorial2"""
    # open and read source
    source0 = plugin_test_dir + "2011072100_006_eta_small"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon2_df).compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_4.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resultScalarVectorial2_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_4"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_5(plugin_test_dir,latlon_df):
    """Test #5 : test_nearest"""
    # open and read source
    source0 = plugin_test_dir + "tape10.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_df,interpolation_type='nearest').compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType NEAREST] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_5.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resultNearest_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_5"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_6(plugin_test_dir,latlon_df):
    """Test #6 : test_linear"""
    # open and read source
    source0 = plugin_test_dir + "tape10.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_df,interpolation_type='bi-linear').compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-LINEAR] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_6.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resultLinear_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_6"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_7(plugin_test_dir,latlon_with_grid_df):
    """Test #7 : test_withGridInCsv"""
    # open and read source
    source0 = plugin_test_dir + "tape10.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_with_grid_df,interpolation_type='nearest').compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType NEAREST] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_7.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "result_withGridInCsv_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_7"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_8(plugin_test_dir,latlon_extrapolation_df):
    """Test #8 : test_extrapolationValue"""
    # open and read source
    source0 = plugin_test_dir + "tape10.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_extrapolation_df,interpolation_yype='bi-cubic', extrapolation_type='value', value=999.9).compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-CUBIC --extrapolationType VALUE=999.9] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_8.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "result_extrapolValue_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_8"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_9(plugin_test_dir,latlon_extrapolation_df):
    """Test #9 : test_negativeValue"""
    # open and read source
    source0 = plugin_test_dir + "tape10.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_extrapolation_df,interpolation_yype='bi-cubic', extrapolation_type='value', value=99.9).compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-CUBIC --extrapolationType VALUE=-99.9] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_9.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "result_negativeValue_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_9"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_10(plugin_test_dir,latlon_extrapolation_df):
    """Test #10 : test_extrapolationMax"""
    # open and read source
    source0 = plugin_test_dir + "tape10.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_extrapolation_df,interpolation_yype='bi-cubic', extrapolation_type='maximum').compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-CUBIC --extrapolationType MAXIMUM] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_10.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "result_extrapolMax_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_10"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_11(plugin_test_dir,latlon_extrapolation_df):
    """Test #11 : test_extrapolationMin"""
    # open and read source
    source0 = plugin_test_dir + "tape10.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_extrapolation_df,interpolation_yype='bi-cubic', extrapolation_type='minimum').compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-CUBIC --extrapolationType MINIMUM] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_11.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "result_extrapolMin_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_11"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_12(plugin_test_dir,stationsdf_df):
    """Test #12 : test_stations"""
    # open and read source
    source0 = plugin_test_dir + "2011072100_006_eta_small"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,stationsdf_df).compute()
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName GZ,UU,VV,TT] >> [GetDictionaryInformation --dataBase STATIONS --outputAttribute LAT,LON --advancedRequest SELECT_Latitude,Longitude_FROM_STATIONSFB] >> [InterpolationHorizontalPoint] >> [Zap --metadataZappable --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_12.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "result_stations_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_12"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_13(plugin_test_dir,latlon_df):
    """Test #13 : test with 2 grids and 3 fields on each grid"""
    # open and read source
    source0 = plugin_test_dir + "2011110112_045_small"
    src_df0 = StandardFileReader(source0).to_pandas()

    source1 = plugin_test_dir + "2011110112_048_small"
    src_df1 = StandardFileReader(source1).to_pandas()

    src_df = pd.concat([src_df0,src_df1],ignore_index=True) 

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df,latlon_df).compute()
    #[ReaderStd --input {sources[0]}] >> [ReaderStd --input {sources[1]}] >> [ReaderCsv --input {sources[2]}] >> [InterpolationHorizontalPoint] >> [Zap --metadataZappable --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --makeIP1EncodingWorkWithTests]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_13.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "result_2grids_3fields_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_13"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_14(plugin_test_dir):
    """Test #14 : test_DanielPoints"""
    # open and read source
    source0 = plugin_test_dir + "2012022712_012_glbdiag"
    src_df0 = StandardFileReader(source0).to_pandas()

    source1 = plugin_test_dir + "latlong_stn_ALL.fst"
    src_df1 = StandardFileReader(source1).to_pandas()

    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,src_df1,interpolation_type='bi-cubic',extrapolation_type='value', value=999.9).compute()
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName 2Z] >> [ReaderStd --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-CUBIC --extrapolationType VALUE=999.9] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_14.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "result_DanielPoints_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_14"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_15(plugin_test_dir):
    """Test #15 : test_northPole_southPole"""
    # open and read source
    source0 = plugin_test_dir + "2012022712_012_glbdiag"
    src_df0 = StandardFileReader(source0).to_pandas()

    source1 = plugin_test_dir + "latlong_stn_ALL.fst"
    src_df1 = StandardFileReader(source1).to_pandas()


    #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,src_df1,interpolation_type='bi-cubic',extrapolation_type='value', value=999.9).compute()
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName SN] >> [ReaderStd --ignoreExtended --input {sources[1]}] >> [Zap --dateOfOrigin 20110210T215210 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-CUBIC --extrapolationType VALUE=999.9] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_15.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "northSouthPole_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_15"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_16(plugin_test_dir,simple_input_df):
    """Test #16 : Test avec un fichier YinYang"""
    # open and read source
    source0 = plugin_test_dir + "2015072100_240_TTESUUVV_YinYang.std"
    src_df0 = StandardFileReader(source0).to_pandas()

     #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,simple_input_df,interpolation_type='bi-linear',extrapolation_type='value', value=99.9).compute()
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName TT] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20150805T094230 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-LINEAR --extrapolationType VALUE=99.9] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_16.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "InterpGridUtoGridY_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_16"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_17(plugin_test_dir,latlon_yy_df):
    """Test #17 : Test avec un fichier YinYang en entree et des lat-lon sur les grilles Yin et Yang."""
    # open and read source
    source0 = plugin_test_dir + "2015072100_240_TTESUUVV_YinYang.std"
    src_df0 = StandardFileReader(source0).to_pandas()

     #compute InterpolationHorizontalPoint
    df = InterpolationHorizontalPoint(src_df0,latlon_yy_df,interpolation_type='bi-linear',extrapolation_type='value', value=99.9).compute()
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName TT] >> [ReaderCsv --input {sources[1]}] >> [Zap --dateOfOrigin 20150805T094230 --doNotFlagAsZapped] >> [InterpolationHorizontalPoint --interpolationType BI-LINEAR --extrapolationType VALUE=99.9] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_interppoint_reg_17.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "InterpGridU_manyPts_file2cmp.std"
    file_to_compare = "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalPoint/" +  "result_test_17"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


