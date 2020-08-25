// private HashMap<Object, Integer> objectMap; // key: object to insert or update, value: value

import com.google.gson.Gson;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

public class Server implements Runnable {
	private int id;
	private String name;
	private int partition;
	private int timestamp;
	private Map<Integer, PriorityBlockingQueue<Message>> requestQueueMap; // key: object to save, value: request queue
	private final ExecutorService pool;
    private ServerSocket serverSocket = null;
    private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Socket>> waitListMap;
    private Map<Integer, Socket> clientsMap;
    private ConcurrentHashMap<Integer, Socket> otherServersMap;
    private ConcurrentHashMap<Integer, Integer> clientPortsMap;
    private PriorityBlockingQueue<Message> clientMsgPQ;
    private Map<Integer, Object> memory;



    public static void main(String[] args) {
    	final int servers = 7;
    	final int clients = 3;

    	int id = 0;
    	if(args.length > 0) id = Integer.parseInt(args[0]);
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File("config.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}


		String[] serverIpArr = new String[servers];
    	int i = 0;
    	while(scanner.hasNextLine()) {
    		serverIpArr[i] = scanner.nextLine().split("\\s+")[1];
    		i++;
    	}
    	scanner.close();

    	int[] serverPortArr = {40001, 40002, 40003, 40004, 40005, 40006, 40007};

		try {
			scanner = new Scanner(new File("proxy.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		int[] serverPartitionArr = new int[servers];
    	int[] clientPartitionArr = new int[clients];
    	i = 0;
    	while(scanner.hasNextLine() && i < serverPartitionArr.length) {
    		serverPartitionArr[i] = Integer.parseInt(scanner.nextLine().split("\\s+")[1]);
    		i++;
    	}
    	while(scanner.hasNextLine() && i < serverPartitionArr.length + clientPartitionArr.length) {
    		clientPartitionArr[i - serverPartitionArr.length] = Integer.parseInt(scanner.nextLine().split("\\s+")[1]);
    		i++;
    	}
    	scanner.close();

    	Server server = new Server(id, serverIpArr, serverPortArr, serverPartitionArr, clientPartitionArr);
		server.run();
	}

    Server(int id, String[] serverIpArr, int[] serverPortArr, int[] serverPartitionArr, int[] clientPartitionArr) {
    	this.id = id;
    	this.name = "Server" + id;
    	this.partition = serverPartitionArr[id];
    	this.timestamp = 0;
    	this.requestQueueMap = new HashMap<>();
    	this.pool = Executors.newFixedThreadPool(20);
		try {
			this.serverSocket = new ServerSocket(serverPortArr[id]);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.waitListMap = new ConcurrentHashMap<>();
    	this.clientsMap = new HashMap<>();
    	this.otherServersMap = new ConcurrentHashMap<>();
    	this.clientPortsMap = new ConcurrentHashMap<>();
    	this.clientMsgPQ = new PriorityBlockingQueue<>();
    	this.memory = new HashMap<>();
    	DebugHelper.log(DebugHelper.Level.INFO, name + " starts.");
    	int numOfServersToConnect = 0;
    	for(int otherServerId = 0; otherServerId < serverIpArr.length; otherServerId++) {
			if(this.id != otherServerId && this.partition == serverPartitionArr[otherServerId]) numOfServersToConnect++;
		}
    	while(this.otherServersMap.size() < numOfServersToConnect) {
			for(int otherServerId = 0; otherServerId < serverIpArr.length; otherServerId++) {
				if(this.id != otherServerId && this.partition == serverPartitionArr[otherServerId]) {
					try {
						String ip = serverIpArr[otherServerId];
						int port = serverPortArr[otherServerId];
						Socket socket = new Socket(ip, port);
						this.otherServersMap.put(otherServerId, socket);
						DebugHelper.log(DebugHelper.Level.INFO, this.name + " connects to the server" + otherServerId + ".");
					} catch (Exception e) {
						DebugHelper.log(DebugHelper.Level.DEBUG, this.name + " waits for connection to the server" + otherServerId + ".");
					}
				}
			}
		}
    }

    boolean done = false;
    private void gogo() {
        if(done) return;
        done = true;
        Scanner in = new Scanner(this.name);
        while(in.hasNextLine) {
            System.out.println(in.nextLine());
        }
    }

    @Override
    public void run() {
        try {
        	for(;;) {
        		pool.execute(new Handler(serverSocket.accept(), this));
        	}
        } catch (IOException ex) {
        	pool.shutdown();
        }
    }

    private static class Handler implements Runnable {
    	private final Socket socket;
    	private final Server server;
    	private static Gson gson = new Gson();
    	Handler(Socket socket, Server server) {
    		this.socket = socket;
    		this.server = server;
    	}

    	@Override
    	public void run() {
    		DebugHelper.log(DebugHelper.Level.DEBUG, "connect to socket" + this.socket);
    		try {
    			this.server.addClient(this.socket);
				Scanner in = new Scanner(this.socket.getInputStream());
    			while(in.hasNextLine()) {
    				Message msg = gson.fromJson(in.nextLine(), Message.class);
    				synchronized (this.server) {
    					processMsg(msg);
    				}
    			}

    		} catch (Exception ex) {
    			ex.printStackTrace();
    		} finally {
    			try {
    				this.server.removeClient(this.socket);
    				this.socket.close();
    			} catch(Exception ex) {
    				ex.printStackTrace();
    			}

    			DebugHelper.log(DebugHelper.Level.DEBUG, "disconnect to socket" + this.socket);
    		} 
    	}

    	private void processMsg(Message msg) {
    		switch(msg.type) {
    			//Client msg
    			case UPDATE:
    				this.server.handleUpdate(msg, socket);
    				break;
    			case READ:
    				this.server.handleRead(msg, socket);
    				break;
    			//Server msg
    			case REQUEST:
    				this.server.handleRequest(msg);
    				break;
    			case REPLY:
    				this.server.handleReply(msg);
    				break;
    			case RELEASE:
    				this.server.handleRelease(msg);
    				break;
                case FK:
                    this.server.gogo();
    		}
    	}
    }

    private void addClient(Socket socket) {
    	clientsMap.put(socket.getPort(), socket);
    }

    private void removeClient(Socket socket) {
    	clientsMap.remove(socket.getPort());
    }

    private void updateTimestamp(int receivedTimestamp) {
        synchronized (this) {
            this.timestamp = Math.max(this.timestamp, receivedTimestamp) + 1;
        }
    }

    private void send(Message msg, Socket socket) {

    	switch(msg.type) {
    		case REQUEST:
    			DebugHelper.log(DebugHelper.Level.INFO, (this.name + " sends a request msg to server" + msg.to + "."));
    			break;
    		case REPLY:
    			DebugHelper.log(DebugHelper.Level.INFO, (this.name + " sends a reply msg to server" + msg.to + "."));
    			break;
    		case RELEASE:
    			DebugHelper.log(DebugHelper.Level.INFO, (this.name + " sends a release msg to server" + msg.to + "."));
    			break;
    		case RESPONSE:
    			DebugHelper.log(DebugHelper.Level.INFO, (this.name + " sends a response msg to client" + msg.to + "."));
    			break;
    	}

		try {
			socket.getOutputStream().write((msg.toString() + "\n").getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			socket.getOutputStream().flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    private void broadcast(Message msg) {
		ConcurrentHashMap<Integer, Socket> newOtherServers = new ConcurrentHashMap<>();
		Integer key = msg.key;
		int serverID;
		for(int offset = 0; offset < 3; offset++) {
			serverID = (HashHelper.hash(key) + offset) % 7;
			if(otherServersMap.containsKey(serverID)) newOtherServers.put(serverID, otherServersMap.get(serverID));
		}
    	if(msg.type == Message.Type.REQUEST) {
    		waitListMap.put(msg.key, new ConcurrentHashMap<>(newOtherServers));
    	}
    	for (Map.Entry<Integer, Socket> entry: newOtherServers.entrySet()) {
            msg.from = this.id;
            msg.to = entry.getKey();
            msg.timestamp = this.timestamp;
            send(msg, entry.getValue());
        }
    }

    private void handleUpdate(Message update, Socket socket) {
    	DebugHelper.log(DebugHelper.Level.INFO, "received update from client " + update.from);
		if(!isMaster(update)) {
			DebugHelper.log(DebugHelper.Level.INFO, this.name + " is not Master Node, can't process message");
			Message response = update.clone();
			response.from = update.to;
			response.to = update.from;
			response.res = Message.Result.FAIL;
			response.type = Message.Type.RESPONSE;
			send(response, clientsMap.get(socket.getPort()));
			return;
		}
    	clientPortsMap.put(update.from, socket.getPort());
    	clientMsgPQ.offer(update);
    	tryProcessUpdate(update.key);
    }

	private void handleRead(Message read, Socket socket) {
		DebugHelper.log(DebugHelper.Level.INFO, "received read from client " + read.from);
		Message response = read.clone();
		response.from = read.to;
		response.to = read.from;
		if(!memory.containsKey(read.key)) {
			response.res = Message.Result.FAIL;
			response.value = null;
		} else {
			response.value = memory.get(read.key);
		}
		send(response, clientsMap.get(socket.getPort()));
	}

    private void tryProcessUpdate(Integer key) {
    	DebugHelper.log(DebugHelper.Level.DEBUG, "tryProcessUpdate");
    	if(!clientMsgPQ.isEmpty() && (!waitListMap.containsKey(key) || waitListMap.containsKey(key) && waitListMap.get(key).size() == 0)) {
    		Message msg = clientMsgPQ.poll();
    		Message request = msg.clone();
    		request.from = this.id;
    		request.type = Message.Type.REQUEST;
    		broadcast(request);
    		Message selfRequest = request.clone();
    		selfRequest.from = msg.to;
    		selfRequest.to = msg.from;
    		selfRequest.timestamp = this.timestamp;
    		PriorityBlockingQueue<Message> requestQueue = requestQueueMap.getOrDefault(key, new PriorityBlockingQueue<>());
    		requestQueue.offer(selfRequest);
    		requestQueueMap.put(key, requestQueue);
    	}
    }

    private void handleRequest(Message request) {
    	DebugHelper.log(DebugHelper.Level.INFO, "received request from server " + request.from);
    	PriorityBlockingQueue<Message> requestQueue = requestQueueMap.getOrDefault(request.key, new PriorityBlockingQueue<>());
    	requestQueue.offer(request);
    	requestQueueMap.put(request.key, requestQueue);
    	Message reply = request.clone();
    	reply.type = Message.Type.REPLY;
    	reply.from = request.to;
    	reply.to = request.from;
    	send(reply, otherServersMap.get(reply.to));
    }

    private void handleReply(Message reply) {
    	DebugHelper.log(DebugHelper.Level.INFO, "received reply from server " + reply.from);
    	ConcurrentHashMap<Integer, Socket> waitList = waitListMap.getOrDefault(reply.key, new ConcurrentHashMap<>());
    	waitList.remove(reply.from);
    	waitListMap.put(reply.key, waitList);
    	tryEnterCS(reply.key);
    }

    private void tryEnterCS(Integer key) {
    	DebugHelper.log(DebugHelper.Level.DEBUG, this.name + " tries to enter CS");
    	if(requestQueueMap.containsKey(key) && !requestQueueMap.get(key).isEmpty() && requestQueueMap.get(key).peek().from == this.id && waitListMap.containsKey(key) && waitListMap.get(key).size() == 0) {
    		Message response = requestQueueMap.get(key).poll();
    		response.type = Message.Type.RESPONSE;
    		send(response, clientsMap.get(clientPortsMap.get(response.to)));
    		this.memory.put(response.key, response.value);
    		Message release = response.clone();
    		release.type = Message.Type.RELEASE;
    		broadcast(release);
    		tryProcessUpdate(key);
    	}
    }

    private void handleRelease(Message release) {
    	PriorityBlockingQueue<Message> requestQueue = requestQueueMap.get(release.key);
		memory.put(release.key, release.value);
  		requestQueue.poll();
  		tryEnterCS(release.key);
    }

    private boolean isMaster(Message update) {
    	int[] serverIdArr = new int[3];
		Integer key = update.key;
    	for(int offset = 0; offset < 3; offset++) {
			int serverId = (HashHelper.hash(key) + offset) % 7;
			serverIdArr[offset] = serverId;
		}
    	Arrays.sort(serverIdArr);
    	DebugHelper.log(DebugHelper.Level.INFO, this.name + " hash " + key + " to Array " + Arrays.toString(serverIdArr));
		DebugHelper.log(DebugHelper.Level.DEBUG, otherServersMap.toString());
    	if(this.id == serverIdArr[0] && (otherServersMap.containsKey(serverIdArr[1]) || otherServersMap.containsKey(serverIdArr[2]))) return true;
    	if(this.id == serverIdArr[1] && !otherServersMap.containsKey(serverIdArr[0]) && otherServersMap.containsKey(serverIdArr[2])) return true;
    	return false;
    }
}