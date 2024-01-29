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
                Message message = getMessageFromQueue(inputMessageQueue);
                if(message != null) {
                    processMessage(message);
                    enqueueWhenNext(message, outputMessageQueue);
                } else {
                    System.out.println("There is no message to process for thread: " + threadNumber);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private synchronized Message getMessageFromQueue(Queue<Message> messageQueue) {
        Message message = null;
        if(!messageQueue.isEmpty()) {
            message = inputMessageQueue.poll();
        }
        return message;
    }

    private void processMessage(Message message) throws InterruptedException {
        System.out.println("Thread: "
                + threadNumber
                + " started processing message id: "
                + message.getId()
                + " with processing time of: "
                + message.getProcessingTime()
        );
        Thread.sleep(message.getProcessingTime());
    }

    private void enqueueWhenNext(Message message, Queue<Message> outputMessageQueue) throws InterruptedException {
        while(true) {
            synchronized (outputMessageQueue) {
                if (message.getId() == nextIdToProcess.get()) {
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
                    outputMessageQueue.notifyAll();
                    break;
                } else {

                    System.out.println("Thread id: "
                            + threadNumber
                            + " processing message id: "
                            + message.getId()
                            + " is waiting");

                    outputMessageQueue.wait();
                }
            }
        }
    }
}