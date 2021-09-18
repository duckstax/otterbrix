# Start Recommendation

All tasks has postfix **task**:

- subprocess_task - executes "ls" command.
- create_audio_wave_task - creates random wav audio.
- async_task - sleeps 1 sec and return 1
- request_task - gets request to google.com.
- yield_task - creates generator and calculate sum.
- async_yield_task - creates async generator and calculate sum.
- start_many_tasks - starts 1000 subprocess_task.

Start RabbitMQ and Celery container:

```shell script
sudo docker-compose up
```
