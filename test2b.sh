red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`

all_cases=(
    "TestBasic2B"
	"TestConcurrent2B"
	"TestUnreliable2B"
	"TestOnePartition2B"
	"TestManyPartitionsOneClient2B"
	"TestManyPartitionsManyClients2B"
	"TestPersistOneClient2B"
	"TestPersistConcurrent2B"
	"TestPersistConcurrentUnreliable2B"
	"TestPersistPartition2B"
	"TestPersistPartitionUnreliable2B"
)
function go_test() {
    GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run $1
}

function do_test() {
    for (( i=1; i <= 5; ++i )) do
        echo "**********start running test $i**********"
        rm -f /tmp/test$i.go.log && touch /tmp/test$i.go.log
        for t in ${all_cases[@]}; do
            echo "running testcase $t..."
            go_test $t >> /tmp/test$i.go.log
            res=$(grep 'FAIL' /tmp/test$i.go.log)
            if [ "$ret" != '' ]
            then
                echo "${red}failed to run testcase $t${reset}"
                break
            else
                echo "${green}succeed to run testcase $t${reset}"
            fi
        done
        echo "**********finish running test $i**********"
    done
}

function test_concurrent() {
    # todo: impl
    echo "hasn't been implemented"
}

if [ "$case" = 'result' ]
then
    get_result
elif [ "$case" = 'all' ]
then
    do_test
elif [ "$case" != '' ]
then
    res=''
    i=0
    while [ $i -eq 0 ] || ( [ $i -lt 5 ] && [ "$res" = '' ] )
    do
        go_test $case > /tmp/test.go.log
        res=$(grep 'FAIL' /tmp/test.go.log)
        ((i++))
        echo "test $i finished"
    done
else
    test_concurrent
fi