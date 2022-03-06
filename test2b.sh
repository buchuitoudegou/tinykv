function get_result() {
    for (( i=1; i <= 10; ++i)) do
        ret=$(cat /tmp/test$i.go.log | grep 'FAIL')
        if [ "$ret" != '' ]
        then
            echo "**********************"
            echo "fail to run test $i"
            echo "$ret"
            echo "**********************"
        else
            echo "succed to run test $i"
        fi
    done
}

function do_test() {
    for (( i=1; i <= 10; ++i )) do
        make project2b > /tmp/test$i.go.log 2>&1 &
    done
}

if [ "$case" = 'result' ]
then
    get_result
elif [ "$case" = 'all' ]
then
    do_test
else
    res=''
    i=0
    while [ $i -eq 0 ] || ( [ $i -lt 20 ] && [ "$res" = '' ] )
    do
        go test -count=1 -run $case github.com/pingcap-incubator/tinykv/kv/test_raftstore > /tmp/test.go.log
        res=$(grep 'FAIL' /tmp/test.go.log)
        ((i++))
        echo "test $i finished"
    done
fi