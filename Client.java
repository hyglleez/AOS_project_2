import com.google.gson.Gson;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class Client {
	private final int id; // client id
	private final Map<Integer, Socket> serversMap; // key: serverId, value: socket
	private Map<Socket, Scanner> scannersMap; // socket scanners
	private int timestamp;
	private Gson gson;
	private String name;
	private int partition;
	private Set<Integer> keySet;

	public static void main(String[] args) throws IOException, InterruptedException {
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

    	Client client = new Client(id, serverIpArr, serverPortArr, serverPartitionArr, clientPartitionArr);
    	client.run();
    }

	/**
	* @param serverIpArr server ip address array
	* @param serverPortArr server port array
	* @param serverPartitionArr server partition array
	* @param clientPartitionArr client partition array
	*/
	public Client(int id, String[] serverIpArr, int[] serverPortArr, int[] serverPartitionArr, int[] clientPartitionArr) {
		this.id = id;
		this.serversMap = new HashMap<>();
		this.scannersMap = new HashMap<>();
		this.timestamp = 0;
		this.gson = new Gson();
		this.name = "Client" + id;
		this.partition = clientPartitionArr[id];
		this.keySet = new HashSet<>();
		int numOfServersToConnect = 0;
		for(int serverPartition: serverPartitionArr) {
			if(this.partition == serverPartition) numOfServersToConnect++;
		}
		DebugHelper.log(DebugHelper.Level.INFO , this.name + " starts at time: " + timestamp + ".");

		while(serversMap.size() < numOfServersToConnect) {
			for(int i = 0; i < serverIpArr.length; i++) {
				if(serverPartitionArr[i] != this.partition) {
					DebugHelper.log(DebugHelper.Level.INFO, this.name + " failed to connect the server" + i + ".");
					continue;
				}
				try {
					String ip = serverIpArr[i];
					int port = serverPortArr[i];
					Socket socket = new Socket(ip, port);
					serversMap.put(i, socket);
					scannersMap.put(socket, new Scanner(socket.getInputStream()));
					DebugHelper.log(DebugHelper.Level.INFO, this.name + " connects to the server" + i + ".");
				} catch (Exception e) {
					DebugHelper.log(DebugHelper.Level.DEBUG, this.name + " waits for connection to the server" + i + ".");
				}
			}
		}
	}

	private void run() throws InterruptedException, IOException {
		try {
			scanner = new Scanner(new File(name));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		Integer key = new Random().nextInt(100);
//			Integer value = new Random().nextInt(100);
		Integer value = key;
		for(int serverId = 0; serverId < 7; serverId++) {
			Message msg = new Message(Message.Type.FK, Message.Result.SUCCESS, key, value, timestamp, this.id, serverId);
			socket.getOutputStream().write((msg.toString() + "\n").getBytes());
	    	socket.getOutputStream().flush();
		}

		while(Scanner.hasNextLine()) {
			System.out.println(Scanner.nextLine());
			Thread.sleep(100);
		}
	} 

	private void handleMsg(Message msg) {
		if(msg.res == Message.Result.FAIL) return;
		DebugHelper.log(DebugHelper.Level.DEBUG, this.name + "handle msg " + msg + ".");
		DebugHelper.log(DebugHelper.Level.INFO, this.name + " receives a " + (msg.res == Message.Result.SUCCESS? "success": "error") + " message from server" + msg.from + ", and the object value is " + msg.value + ".");
		if(msg.res == Message.Result.SUCCESS) keySet.add(msg.key);
		this.timestamp = Math.max(this.timestamp, msg.timestamp) + 1;
	}

	private void shutDown() {
		for(Map.Entry<Integer, Socket> entry: serversMap.entrySet()) {
			Socket socket = entry.getValue();
			try {
				socket.shutdownOutput();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		DebugHelper.log(DebugHelper.Level.INFO, this.name + " has been shut down.");
	}
}