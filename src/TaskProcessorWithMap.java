import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class TaskProcessorWithMap implements Runnable {

    private final AtomicLong nextIdToProcess;
    private final Map<Long,Message> messageMap;
    private Queue<Message> inputMessageQueue;
    private Queue<Message> outputMessageQueue;
    private long threadNumber;
    public TaskProcessorWithMap(Queue<Message> inputMessageQueue, Queue<Message> outputMessageQueue, Map<Long,Message> messageMap, int threadNumber, AtomicLong nextIdToProcess) {
        this.inputMessageQueue = inputMessageQueue;
        this.outputMessageQueue = outputMessageQueue;
        this.messageMap = messageMap;
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
                    messageMap.put(message.getId(), message);
                    System.out.println("Message id: " +  message.getId() + " added to a temp map by thread " + threadNumber);

                    while(true) {
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
                                break;
                            } else {
                                synchronized (messageMap) {
                                    if (messageMap.containsKey(nextIdToProcess.get())) {
                                        Message messageToPush = messageMap.get(nextIdToProcess.get());
                                        outputMessageQueue.add(messageToPush);
                                        messageMap.remove(nextIdToProcess.get());
                                        System.out.println("Message id: " + nextIdToProcess.get() + " removed from map");
                                        nextIdToProcess.incrementAndGet();
                                    }
                                    break;
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
