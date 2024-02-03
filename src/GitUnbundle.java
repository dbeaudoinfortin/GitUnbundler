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


	private static final String RAW_EXPORT_PATH     = "C:\\git\\@hashed\\";
	private static final String BUNDLE_PATH         = "C:\\git\\";
	private static final String UNBUNDLED_PATH      = "C:\\git\\repos\\";
	
	private static final Path bundlePath    = Paths.get(BUNDLE_PATH); 
	private static final Path unbundledPath = Paths.get(UNBUNDLED_PATH);
	
	private static final int                THREAD_COUNT = 24;
	private static final ThreadPoolExecutor THREAD_POOL = new ThreadPoolExecutor(THREAD_COUNT,THREAD_COUNT,100l,TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
	private static final AtomicInteger      THREAD_ID_COUNTER = new AtomicInteger(0);
	
	public static void main(String[] args) throws IOException {
		moveFiles();
		createDirs();
		unbundle();
		
		THREAD_POOL.shutdown();
		System.out.println("All done. Bye :)");
	}
	
	private static void unbundle() throws IOException {
		
		List<Future<?>> futures = new ArrayList<Future<?>>();
		
		Files.list(bundlePath).forEach(path -> {
			File bundleFile = path.toFile();
			if (!bundleFile.isFile()) return;
			
			String fileName = bundleFile.getName();
			int index = fileName.indexOf(".bundle");
			if (index < 1) return;
			
			File unbundledDir = unbundledPath.resolve(bundleFile.getName().substring(0, index)).toFile();
			
			futures.add(THREAD_POOL.submit(new Runnable() {
				 
				 @Override
		            public void run() {
					 
					 Process process = null;
					 BufferedReader stdInput = null;
					 BufferedReader stdError = null;
					 
					 int threadId = THREAD_ID_COUNTER.getAndIncrement();
					 
					 try {
						System.out.println(threadId + ":: Creating git repo in: " + unbundledDir);
						process = Runtime.getRuntime().exec("git init", null, unbundledDir);
						stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
						stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
						
						if(process.waitFor() > 0) {
							System.out.println(threadId + ":: Git repo creation failed in: " + unbundledDir);
							printErrorMessages(threadId, stdInput, stdError);
							return;
						} else {
							System.out.println(threadId + ":: Git repo created in: " + unbundledDir);
							printErrorMessages(threadId, stdInput, stdError);
						}
					 } catch (Exception e) {
						System.out.println(threadId + ":: ERROR during git init command: " + e.getMessage());
						e.printStackTrace();
					 } finally {
						if (null != process) process.destroy();
					 }
					 
					 try {
							
						System.out.println(threadId + ":: Unbundling file: " + bundleFile);
						String cmd = "git pull \"" + bundleFile.getAbsolutePath() + "\"";
						System.out.println(threadId + ":: Running command: " + cmd);
						
						process = Runtime.getRuntime().exec(cmd, null, unbundledDir);
						stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
						stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
						printErrorMessages(threadId, stdInput, stdError);
						
						if(process.waitFor() > 0) {
							System.out.println(threadId + ":: Unbundling failed for bundle: " + bundleFile);
						} else {
							System.out.println(threadId + ":: Unbundling succeeded, deleting bundle: " + bundleFile);
							bundleFile.delete();
						}
						printErrorMessages(threadId, stdInput, stdError);
					} catch (Exception e) {
						System.out.println(threadId + ":: ERROR: " + e.getMessage());
						e.printStackTrace();
					} finally {
						if (null != process) process.destroy();
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
		
		Files.list(bundlePath).forEach(path -> {
			File bundleFile = path.toFile();
			if (!bundleFile.isFile()) return;
			
			String fileName = bundleFile.getName();
			int index = fileName.indexOf(".bundle");
			if (index < 1) return;
			
			futures.add(THREAD_POOL.submit( new Runnable() {
				
				@Override
	            public void run() {
					int threadId = THREAD_ID_COUNTER.getAndIncrement();
					
					
					
					File newDir = unbundledPath.resolve(bundleFile.getName().substring(0, index)).toFile();
					if(newDir.exists()) {
						try {
							if(FileUtils.isEmptyDirectory(newDir)) {
								System.out.println(threadId + ":: Directory already exists and is empty: " + newDir);
							} else {
								System.out.println(threadId + ":: Directory already exists, deleting: " + newDir);
								FileUtils.deleteDirectory(newDir);
							}
							
						} catch (IOException e) {
							System.out.println(threadId + ":: ERROR: " + e.getMessage());
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
				Files.move(path, Paths.get(BUNDLE_PATH, file.getName()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

}
