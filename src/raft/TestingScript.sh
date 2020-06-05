for i in {1..100};
do
  echo "Test Number $i "
  echo "Test Number $i " >> temp.txt
	go test  >> temp.txt
done