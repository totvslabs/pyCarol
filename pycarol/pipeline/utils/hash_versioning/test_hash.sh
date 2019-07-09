python tst_bytecodes_output.py > bytecodes_output1
python tst_bytecodes_output.py > bytecodes_output2
python tst_hash_output.py > hash_output1
python tst_hash_output.py > hash_output2

diff -y --suppress-common-lines bytecodes_output1 bytecodes_output2 | wc -l
diff -y --suppress-common-lines hash_output1 hash_output2 | wc -l
# vimdiff output1 output2
