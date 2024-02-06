import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;

public class GitUnbundle {

	
	private static final String RAW_EXPORT_PATH     = "C:\\git\\test\\test.tar";
	private static final String BUNDLE_PATH         = "C:\\git\\";
	private static final String UNBUNDLED_PATH      = "C:\\git\\repos\\";
	
	private static final Path rawExportPath = Paths.get(RAW_EXPORT_PATH);
	private static final Path bundlePath    = Paths.get(BUNDLE_PATH); 
	private static final Path unbundledPath = Paths.get(UNBUNDLED_PATH);
	
	private static final int                THREAD_COUNT = 32;
	private static final ThreadPoolExecutor THREAD_POOL = new ThreadPoolExecutor(THREAD_COUNT,THREAD_COUNT,100l,TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
	private static final AtomicInteger      THREAD_ID_COUNTER = new AtomicInteger(0);
	
	private static final int TAR_BUFFER_SIZE = 1024*1024*10; //File is big
	
	public static void main(String[] args) throws IOException {
		try {
			flattenRawExport(rawExportPath, bundlePath);
			createBundleDirs();
			unbundle();
			
			THREAD_POOL.shutdown();
			System.out.println("All done. Bye :)");
		} catch (Throwable t) {
			System.out.println("Error: " + t.getMessage());
			t.printStackTrace();
		}
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
					 } catch (Throwable t) {
						System.out.println(threadId + ":: ERROR during git init command: " + t.getMessage());
						t.printStackTrace();
						return; //Don't throw a runtime exception, let the other threads run
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
						
					} catch (Throwable t) {
						System.out.println(threadId + ":: ERROR during git unbundling: " + t.getMessage());
						t.printStackTrace(); //Don't throw a runtime exception, let the other threads run
					} finally {
						if (null != process) process.destroy();
					}
				}
			}));
		});
		
		waitForTaskCompletion(futures);
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
	
	private static void createBundleDirs() throws IOException {
		List<Future<?>> futures = new ArrayList<Future<?>>();
		
		Files.list(bundlePath).forEach(path -> {
			File bundleFile = path.toFile();
			if (!bundleFile.isFile()) return;
			
			String fileName = bundleFile.getName();
			int index = fileName.indexOf(".bundle");
			if (index < 1) return;
			
			futures.add(THREAD_POOL.submit(new Runnable() {
				
				@Override
	            public void run() {
					int threadId = THREAD_ID_COUNTER.getAndIncrement();
					
					try {
						File newDir = unbundledPath.resolve(bundleFile.getName().substring(0, index)).toFile();
						if(newDir.exists()) {
							if(FileUtils.isEmptyDirectory(newDir)) {
								System.out.println(threadId + ":: Directory already exists and is empty: " + newDir);
							} else {
								System.out.println(threadId + ":: Directory already exists, deleting: " + newDir);
								FileUtils.deleteDirectory(newDir);
							}
						}
						System.out.println(threadId + ":: Creating dir: " + newDir);
						newDir.mkdir();
					} catch(Throwable t) {
						System.out.println(threadId + ":: ERROR during directory creation: " + t.getMessage());
						throw new RuntimeException(t); //Don't keep processing
					}
				}
			}));
		});
		
		waitForTaskCompletion(futures);
		System.out.println("All directories created.");
	}
	
	private static void flattenRawExport(Path rootPath, Path flattenedPath) throws IOException {
		
		File rootFile = rootPath.toFile();
		if(!rootFile.exists())  {
			System.out.println("Warning: Raw export path does not exist: " + rootPath);
			return; //Allow continue
		}
		
		if(rootFile.isDirectory()) {
			flattenDirStructure(rootPath, flattenedPath);
		} else if (rootFile.getName().toLowerCase().endsWith(".tar")) {
			extractTarFile(rootPath, flattenedPath);
			//No exception means we can now delete the file.
			System.out.println("Deleting waw export file: " + rootPath);
			rootFile.delete();
		} else {
			System.out.println("Unrecognized raw export file. Expected a .tar file, got: " + rootPath);
		}
	}
	
	private static void extractTarFile(Path rootPath, Path flattenedPath) throws IOException {
		List<Future<?>> futures = new ArrayList<Future<?>>();
		
		//Used for co-ordination between threads
		Set<String> entrySet = ConcurrentHashMap.newKeySet(1000);
		
		for (int i = 0; i < THREAD_COUNT; i++) {
			
			futures.add(THREAD_POOL.submit(new Runnable() {
				
				@Override
				public void run()  {
					int threadId = THREAD_ID_COUNTER.getAndIncrement();
					
					//Open the tar file once per thread, this may lead to a lot of contention and memory usage
					//if the number of concurrent threads is high
					try (FileInputStream fis = new FileInputStream(rootPath.toString());
							BufferedInputStream bis = new BufferedInputStream(fis, TAR_BUFFER_SIZE);
							TarArchiveInputStream tarStream = new TarArchiveInputStream(fis)) {
				
						System.out.println("Reading tar file: " + rootPath);
						TarArchiveEntry entry;
					    while ((entry = tarStream.getNextEntry()) != null) {
					    	
					    	if(!entrySet.add(entry.getName())) continue; //Another thread took this file
					    			
					    	System.out.println("Looking at: " + entry.getName());
					    	if(entry.isFile() && entry.getName().endsWith(".bundle")) {
					    		System.out.println("Matched bundle file: " + entry.getName());
					    		
					    		//Found a match, now extract it from the tar file
					    		Path entryPath = Paths.get(entry.getName()).getFileName();
					    		Path target = Paths.get(flattenedPath.toString(), entryPath.toString());
					    		File targetFile = target.toFile();
					    		
					    		if (targetFile.exists()) {
									if(!targetFile.isDirectory() && (targetFile.length() == entry.getRealSize())) {
										System.out.println("Bundle file already exists, skipping: " + target);
									} else {
										System.out.println("Bundle file conflicts with existing file or directory: " + target);
										throw new FileAlreadyExistsException(target.toString());
									}
					    		} else {
					    			System.out.println("Copying bundle file "  + entry.getName() + " to path " + flattenedPath);
						    		Files.copy(tarStream, target);
					    		}
					    	}
					    }
					} catch(Throwable t) {
						System.out.println(threadId + ":: ERROR tar file extraction: " + t.getMessage());
						throw new RuntimeException(t); //Don't keep processing
					}
				}
			}));
		}
		
		waitForTaskCompletion(futures);
	}
	
	private static void flattenDirStructure(Path rootPath, Path flattenedPath) throws IOException {
		//Walk the entire directory tree, up to 4 deep, looking for bundle files
		System.out.println("Walking directory tree, starting at: " + rootPath);
		Files.find(rootPath, 4, (treePath, attr) -> {
			System.out.println("Looking at: " + treePath);
			if(treePath.toString().toLowerCase().endsWith(".bundle")) {
				System.out.println("Matched bundle file: " + treePath);
				return true;
			}
			return false;
		}).forEach(matchedPath -> {
			//Move all the bundle files that we matched to the flattened path
			System.out.println("Moving bundle file " + matchedPath + " to path " + flattenedPath);
			File matchedFile = matchedPath.toFile();
			Path target = Paths.get(flattenedPath.toString(), matchedFile.getName());
			try {
				Files.move(matchedPath,target);
			} catch (FileAlreadyExistsException e) {
				File targetFile = target.toFile();
				if(!targetFile.isDirectory() && (targetFile.length() == matchedFile.length())) {
					System.out.println("Bundle file already exists, skipping: " + target);
				} else {
					System.out.println("Bundle file conflicts with existing file or directory: " + target);
					throw new RuntimeException(e); //Don't keep processing
				}
			} catch (IOException e) {
				throw new RuntimeException(e); //Don't keep processing
			}
		});
	}

	private static void waitForTaskCompletion(List<Future<?>> futures) {
		futures.forEach(f->{
			try {
				f.get();
			} catch (InterruptedException e) {
				System.out.println("Failed to wait for completion of tasks.");
				throw new RuntimeException(e); 
			} catch (ExecutionException e) {
				System.out.println("Task execution failed.");
				throw new RuntimeException(e);
			}
		});
	}
}
