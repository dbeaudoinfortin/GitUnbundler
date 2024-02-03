import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;

public class GitUnbundle {


	private static final String RAW_EXPORT_PATH = "C:\\git\\@hashed\\";
	private static final String UNBUNDLED_REPO_PATH = "C:\\git\\repos\\";
	
	private static final int               THREAD_COUNT = 24;
	private static final ThreadPoolExecutor THREAD_POOL = new ThreadPoolExecutor(THREAD_COUNT,THREAD_COUNT,100l,TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
	private static final AtomicInteger      THREAD_ID_COUNTER = new AtomicInteger(0);
	
	public static void main(String[] args) throws IOException {
		moveFiles();
		createDirs();
		unbundle();
	}
	
	private static void unbundle() throws IOException {
		
		List<Future<?>> futures = new ArrayList<Future<?>>();
		
		Path rootPath = Paths.get(UNBUNDLED_REPO_PATH);
		Files.list(Paths.get(UNBUNDLED_REPO_PATH)).forEach(path -> {
			File file = path.toFile();
			if (!file.isFile()) return;
			String fileName = file.getName();
			int index = fileName.indexOf(".bundle");
			if (index < 1) return;
			File dir = rootPath.resolve(file.getName().substring(0, index)).toFile();
			
			futures.add(THREAD_POOL.submit( new Runnable() {
				 
				 @Override
		            public void run() {
					 int threadId = THREAD_ID_COUNTER.getAndIncrement();
					 try {
							System.out.println(threadId + ":: Creating git repo in: " + dir);
							Process process = Runtime.getRuntime().exec("git init", null, dir);
							BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
							BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
							int exitCode = process.waitFor();
							
							printErrorMessages(threadId, stdInput, stdError);
							System.out.println(threadId + ":: Exit code: " + exitCode);
							if(exitCode > 0) {
								System.out.println(threadId + ":: Git repo creation failed in: " + dir);
								return;
							} else {
								System.out.println(threadId + ":: Git repo created in: " + dir);
							}
							
							System.out.println(threadId + ":: Unbundling file: " + file);
							String cmd = "git pull \"" + file.getAbsolutePath() + "\"";
							System.out.println(threadId + ":: Running command: " + cmd);
							
							process = Runtime.getRuntime().exec("git pull \"" + file.getAbsolutePath() + "\"", null, dir);
							stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
							stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
							exitCode = process.waitFor();
							
							System.out.println(threadId + ":: Exit code: " + exitCode);
							printErrorMessages(threadId, stdInput, stdError);
							
							if(exitCode == 0) {
								System.out.println(threadId + ":: Unbunding succeeded, deleting bundle: " + file);
								file.delete();
							}
							else {
								System.out.println(threadId + ":: Unbunding failed for bundle: " + file);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
			}));
		});
		
		futures.forEach(f->{
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});
		
		System.out.println("All done. Bye :)");
	}
	
	private static void printErrorMessages(int threadId, BufferedReader stdInput, BufferedReader stdError) throws IOException {
		System.out.println(threadId + ":: Exit messages:");
		String s = null;
		while ((s = stdInput.readLine()) != null) {
		    System.out.println(threadId + ":: " + s);
		}
		while ((s = stdError.readLine()) != null) {
		    System.out.println(threadId + ":: " + s);
		}
	}
	
	private static void createDirs() throws IOException {
		List<Future<?>> futures = new ArrayList<Future<?>>();
		
		Path rootPath = Paths.get(UNBUNDLED_REPO_PATH);
		Files.list(Paths.get(UNBUNDLED_REPO_PATH)).forEach(path -> {
			File file = path.toFile();
			if (!file.isFile()) return;
			String fileName = file.getName();
			int index = fileName.indexOf(".bundle");
			if (index < 1) return;
			

			futures.add(THREAD_POOL.submit( new Runnable() {
				
				@Override
	            public void run() {
					int threadId = THREAD_ID_COUNTER.getAndIncrement();
					
					File newDir = rootPath.resolve(file.getName().substring(0, index)).toFile();
					if(newDir.exists()) {
						System.out.println(threadId + ":: Directory already exists, deleting: " + newDir);
						try {
							FileUtils.deleteDirectory(newDir);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					System.out.println(threadId + ":: Creating dir: " + newDir);
					newDir.mkdir();
				}
			}));
		});
		
		futures.forEach(f->{
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});
		
		System.out.println("All directories created.");
	}
	
	private static void moveFiles() throws IOException {
		Path root = Paths.get(RAW_EXPORT_PATH);
		if(!root.toFile().exists()) return;
		
		Files.find(root, 4, (path, attr) -> {
			System.out.println("Looking at: " + path);
			if(path.toString().endsWith(".bundle")) {
				System.out.println("Matched: " + path);
				return true;
				
			}
			return false;
		}).forEach(path -> {
			File file = path.toFile();
			System.out.println("Moving: " + path);
			try {
				Files.move(path, Paths.get(UNBUNDLED_REPO_PATH, file.getName()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

}
