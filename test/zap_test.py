

# -*- coding: utf-8 -*-
import pytest
from fstpy.standardfile import *
from test import TEST_PATH

@pytest.mark.zap
class TestZap:
    
    @pytest.fixture
    def plugin_test_dir(self):
        return TEST_PATH +"Zap/testsFiles/"

    # def test_regtest_1(self,plugin_test_dir):
    #     pass
    #     """Test #1 : Tester l'option --typeOfField avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --typeOfField BLABLABLA]

    #     #write the result
    #     results_file = TMP_PATH + "test_1.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_2(self):
    #     """Test #2 : Tester l'option --run avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --run BLABLABLA]

    #     #write the result
    #     results_file = TMP_PATH + "test_2.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_3(self):
    #     """Test #3 : Tester l'option --ensembleMember avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --ensembleMember BLABLABLA]

    #     #write the result
    #     results_file = TMP_PATH + "test_3.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_5(self):
    #     """Test #5 : Tester l'option --verticalLevel avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --verticalLevel -1]

    #     #write the result
    #     results_file = TMP_PATH + "test_5.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_6(self):
    #     """Test #6 : Tester l'option --verticalLevelType avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --verticalLevelType BLABLABLA]

    #     #write the result
    #     results_file = TMP_PATH + "test_6.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_7(self):
    #     """Test #7 : Tester l'option --forecastHour avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --forecastHour -10]

    #     #write the result
    #     results_file = TMP_PATH + "test_7.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_8(self):
    #     """Test #8 : Tester l'option --forecastHourOnly avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --forecastHourOnly -10]

    #     #write the result
    #     results_file = TMP_PATH + "test_8.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_9(self):
    #     """Test #9 : Tester l'option --userDefinedIndex avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --userDefinedIndex -10]

    #     #write the result
    #     results_file = TMP_PATH + "test_9.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_10(self):
    #     """Test #10 : Tester l'option --nbitsForDataStorage avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --nbitsForDataStorage i65]

    #     #write the result
    #     results_file = TMP_PATH + "test_10.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_11(self):
    #     """Test #11 : Tester l'option --unit avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --unit i65]

    #     #write the result
    #     results_file = TMP_PATH + "test_11.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_12(self):
    #     """Test #12 : Tester l'option --forecastHourOnly avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --forecastHourOnly -10:00:00]

    #     #write the result
    #     results_file = TMP_PATH + "test_12.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_13(self):
    #     """Test #13 : Tester l'option --forecastHourOnly avec une valeur valide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11:00:01] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_13.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_13.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_14(self):
    #     """Test #14 : Tester l'option --forecastHour avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #[ReaderStd --input {sources[0]}] >> [Zap --forecastHour -10:00:00]

    #     #write the result
    #     results_file = TMP_PATH + "test_14.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_15(self):
    #     """Test #15 : Tester l'option --forecastHour avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHour 11:38:00] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_15.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_16(self):
    #     """Test #16 : Tester l'option --forecastHour avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHour 11.633333333] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_16.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_17(self):
    #     """Test #17 : Tester l'option --forecastHourOnly, --timeStepNumber et --lenghtOfTimeStep avec une valeur valide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --lenghtOfTimeStep 1 --timeStepNumber 41880] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_17.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_17.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_18(self):
    #     """Test #18 : Tester l'option --forecastHour, --timeStepNumber et --lenghtOfTimeStep avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.6 --lenghtOfTimeStep 1 --timeStepNumber 41880] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_18.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_19(self):
    #     """Test #19 : Tester l'option --forecastHour, --timeStepNumber et --lenghtOfTimeStep avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --lenghtOfTimeStep 2 --timeStepNumber 41880] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_19.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_20(self):
    #     """Test #20 : Tester l'option --forecastHour, --timeStepNumber et --lenghtOfTimeStep avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --lenghtOfTimeStep 1 --timeStepNumber 41888] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_20.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_21(self):
    #     """Test #21 : Tester l'option --forecastHour et --lenghtOfTimeStep avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --lenghtOfTimeStep 1] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_21.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_22(self):
    #     """Test #22 : Tester l'option --forecastHour et --timeStepNumber avec une valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --timeStepNumber 41880] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_22.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_23(self):
    #     """Test #23 : Tester l'option --forecastHourOnly, --timeStepNumber et --lenghtOfTimeStep avec une valeur valide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11:31:00 --lenghtOfTimeStep 1 --timeStepNumber 41460] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_23.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_23.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_24(self):
    #     """Test #24 : Tester l'option --forecastHourOnly, --timeStepNumber et --lenghtOfTimeStep avec une valeur valide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11:31:00 --lenghtOfTimeStep 60 --timeStepNumber 691] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_24.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_24.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_25(self):
    #     """Test #25 : Tester l'option --forecastHourOnly, --timeStepNumber et --lenghtOfTimeStep avec une valeur valide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11:31:01 --lenghtOfTimeStep 1 --timeStepNumber 41461] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_25.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_25.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_26(self):
    #     """Test #26 : Tester l'option --modificationFlag, avec 2 valeurs valide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=TRUE,BOUNDED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_26.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_26.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_27(self):
    #     """Test #27 : Tester l'option --modificationFlag, avec 1 valeur valide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_27.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_27.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_28(self):
    #     """Test #28 : Tester l'option --modificationFlag, avec 1 valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPEDS=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_28.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_29(self):
    #     """Test #29 : Tester l'option --modificationFlag, avec 1 valeur invalide!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=TRU] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_29.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "nan"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == False)


    # def test_regtest_30(self):
    #     """Test #30 : Tester l'option --modificationFlag, avec FILTERED=TRUE!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag FILTERED=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_30.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_30.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_31(self):
    #     """Test #31 : Tester l'option --modificationFlag, avec INTERPOLATED=TRUE!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag INTERPOLATED=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_31.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_31.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_32(self):
    #     """Test #32 : Tester l'option --modificationFlag, avec UNITCONVERTED=TRUE!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag UNITCONVERTED=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_32.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_32.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_33(self):
    #     """Test #33 : Tester l'option --modificationFlag, avec ALL_FLAGS=TRUE!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ALL_FLAGS=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_33.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_33.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_34(self):
    #     """Test #34 : Tester l'option --modificationFlag, avec ZAPPED!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "resulttest_26.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_34.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_34.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_35(self):
    #     """Test #35 : Tester l'option --modificationFlag, avec ZAPPED and FILTERED!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "resulttest_30.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=FALSE,FILTERED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_35.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_34.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_36(self):
    #     """Test #36 : Tester l'option --modificationFlag, avec FILTERED!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag FILTERED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_36.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_34.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_37(self):
    #     """Test #37 : Tester l'option --modificationFlag, avec INTERPOLATED!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "resulttest_31.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag INTERPOLATED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_37.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_34.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_38(self):
    #     """Test #38 : Tester l'option --modificationFlag, avec UNITCONVERTED!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "resulttest_32.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag UNITCONVERTED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_38.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_34.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_39(self):
    #     """Test #38 : Tester l'option --modificationFlag, avec ZAP et FILTERED!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "zap_fileSrc.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=TRUE,FILTERED=TRUE] >>', '[Zap --modificationFlag ZAPPED=FALSE,FILTERED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_39.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_34.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


    # def test_regtest_40(self):
    #     """Test #40 : Tester l'option --modificationFlag, avec ENSEMBLEEXTRAINFO!"""
    #     # open and read source
    #     source0 = plugin_test_dir + "resulttest_32.std"
    #     src_df0 = StandardFileReader(source0)()


    #     #compute Zap
    #     df = Zap(src_df0).compute()
    #     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ALL_FLAGS=FALSE] >>', '[Zap --modificationFlag ENSEMBLEEXTRAINFO=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #     #write the result
    #     results_file = TMP_PATH + "test_40.std"
    #     StandardFileWriter(results_file, df, erase=True)()

    #     # open and read comparison file
    #     file_to_compare = plugin_test_dir + "resulttest_40.std"

    #     #compare results
    #     res = fstcomp(results_file,file_to_compare)
    #     assert(res == True)


