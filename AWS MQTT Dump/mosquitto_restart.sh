if [ "`ps -aux | grep /usr/sbin/mosquitto`" == "1" ]
then
        echo "mosquitto wasnt running so attempting restart" >> /home/ubuntu/cron.log
        systemctl restart mosquitto
        if [ "`ps -aux | grep /home/ubuntu/main.py`" == "1" ]
        then
                echo "python bridge wasnt running so restarting" >> /home/ubuntu/cron.log
                python main.py
                exit 0
        fi
        echo "python bridge is currently running" >> /home/ubuntu/cron.log
        exit 0
fi
echo "$SERVICE is currently running" >> /home/ubuntu/cron.log
if [ "`ps -aux | grep /home/ubuntu/main.py`" == "1" ]
then
        echo "python bridge wasnt running so attempting restart" >> /home/ubuntu/cron.log
        python main.py
        exit 0
fi
echo "python bridge is currently running" >> /home/ubuntu/cron.log
exit
