import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class TaskProcessorWithoutMap implements Runnable{
    private final AtomicLong nextIdToProcess;
    private Queue<Message> inputMessageQueue;
    private Queue<Message> outputMessageQueue;
    private long threadNumber;

    public AtomicLong getNextIdToProcess() {
        return nextIdToProcess;
    }

    public Queue<Message> getInputMessageQueue() {
        return inputMessageQueue;
    }

    public void setInputMessageQueue(Queue<Message> inputMessageQueue) {
        this.inputMessageQueue = inputMessageQueue;
    }

    public Queue<Message> getOutputMessageQueue() {
        return outputMessageQueue;
    }

    public void setOutputMessageQueue(Queue<Message> outputMessageQueue) {
        this.outputMessageQueue = outputMessageQueue;
    }

    public long getThreadNumber() {
        return threadNumber;
    }

    public void setThreadNumber(long threadNumber) {
        this.threadNumber = threadNumber;
    }

    public TaskProcessorWithoutMap(Queue<Message> inputMessageQueue, Queue<Message> outputMessageQueue, long threadNumber, AtomicLong nextIdToProcess) {
        this.inputMessageQueue = inputMessageQueue;
        this.outputMessageQueue = outputMessageQueue;
        this.threadNumber = threadNumber;
        this.nextIdToProcess = nextIdToProcess;
    }

    @Override
    public void run() {
        System.out.println("Start processing thread:" + this.threadNumber);
        while(true) {
            try {
                Message message = inputMessageQueue.poll();
                if(message != null) {
                    System.out.println("Thread: "
                            + threadNumber
                            + " started processing message id: "
                            + message.getId()
                            + " with processing time of: "
                            + message.getProcessingTime()
                    );
                    Thread.sleep(message.getProcessingTime());
                    while(true) {
                        synchronized (nextIdToProcess){
                            if(message.getId() == nextIdToProcess.get()) {
                                outputMessageQueue.add(message);

                                System.out.println("Message with id: "
                                        + message.getId()
                                        + " with processing time: "
                                        + message.getProcessingTime()
                                        + " added (from thread:"
                                        + threadNumber
                                        + ")"
                                );

                                nextIdToProcess.incrementAndGet();
                                nextIdToProcess.notifyAll();
                                break;
                            } else {

                                System.out.println("Thread id: "
                                            + threadNumber
                                            + " processing message id: "
                                            + message.getId()
                                            + " is waiting");


                                nextIdToProcess.wait();
                            }
                        }
                    }
                } else {
                    System.out.println("There is no message to process for thread: " + threadNumber);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
