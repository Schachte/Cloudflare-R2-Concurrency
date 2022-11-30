rm -rf sample/
mkdir sample
for i in {000..2000}
do
    echo hello > "sample/File${i}.txt"
done