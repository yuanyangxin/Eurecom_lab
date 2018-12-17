/* File LRU.java */

package bufmgr;

import java.util.Date;
import diskmgr.*;
import global.*;

//*****************************************************
/** A history description class. It describes history reference of each page,
 * the time of the K most recent reference to a page and the time of the most
 * recent reference to page
 */
class HistInfo implements GlobalConst{

	  /** The page within file, or INVALID_PAGE if the frame is empty. */
	  public PageId pageNo;

	  /** The history control block (array) for the page in this frame */
	  public long hist[];

	  /** The time of the most recent reference to the page in this frame */
	  public long last;

	  /** Creates a HistDesc object, initialize pageNo and hist, last.
	   */
	  public HistInfo(int k) {

	    pageNo = new PageId();
	    pageNo.pid = INVALID_PAGE;
	    hist = new long[k];
	    last = 0;
	  }
}

//*****************************************************
  /**
   * class LRUK is a subclass of class Replacer using LRUK
   * algorithm for page replacement
   */
public class LRUK extends  Replacer {
	
  /** An setting for time out correlation. */
  private static final int TIME_OUT_PERIOD = 15; 		// microseconds
  
  /** An setting for retained information period. */
  private static final int RETAINED_INFO_SIZE = 20; 	// microseconds

  private long timer;

  /** An array of history descriptors one per page. */
  public HistInfo[] histTable;

  /**
   * private field
   * An array to hold number of frames in the buffer pool
   */

  private int  frames[];
 
  /**
   * private field
   * number of frames used
   */   
  private int  nframes;

  
  /**
   * private field
   * number of frames in buffer pool
   */   
  
  public int numBuffers = mgr.getNumBuffers();
  
  /**
   * private field
   * number of free slot that does not contain history reference
   */   
  private int ninfo = 0;
  
  /**
   * private field
   * size of history table
   */  
  
  public int Hist_size = numBuffers + RETAINED_INFO_SIZE;

  /**
   * This update the current pin page history at the given index in history table.
   * @param hist_index	the index of history table
   * 
   * Based on history reference information of current pinned page, need to solve
   *  2 cases if the pinned page reference history is already in history table or not
   * 
   */
  private void update_Hist(int hist_index) {
	  long correl_period_of_refd_page = 0;
	  int index = hist_index;
	  int rm_info_idx = 0;
	  long max = 0;
	  boolean hist_exist = false;
	  timer = System.nanoTime()/1000;   // microseconds
	  
	  
      for ( int i = 0; i < Hist_size; ++i ) {

      	if (histTable[i].pageNo.pid == mgr.current_pin_page.pid){
	        hist_exist = true;  // History table contain information of current pinned page
      		if (i >= mgr.getNumBuffers()) { // if the current pinned page is not in buffer pool
        		HistInfo histTemp;
        		histTemp = histTable[index];
    	        for ( int j = 1; j < mgr.numLastRef; ++j )
    	        	histTable[index].hist[j] = histTable[index].hist[j-1];	        
    	        histTable[index].hist[0] =  timer;
    	        histTable[index].last =  timer;
    	        histTable[index].pageNo.pid = mgr.current_pin_page.pid;
    	        histTable[i] = histTemp;
    	        break;
      		}
      		else { // if the current pinned page is not in buffer pool
      		   if ((timer - histTable[index].last) > TIME_OUT_PERIOD) { // a new uncorrelated reference
      			   correl_period_of_refd_page = histTable[index].last - histTable[index].hist[0];
      			   for ( int j = 1; j < mgr.numLastRef; ++j )
      				   histTable[index].hist[j] = histTable[index].hist[j-1] + correl_period_of_refd_page;
      			   histTable[index].hist[0] = timer;
      			   histTable[index].last = timer;
      			   break;
      		   }
      		   else { // a new uncorrelated reference
      			   histTable[index].last = timer;
      			   break;
      		   }
      		}
       	}
      }
      if(!hist_exist) { // History table does not contain information of current pinned page
      	if (ninfo >= RETAINED_INFO_SIZE) { // History table full slots
	        for ( int i = numBuffers; i < Hist_size; ++i ) {
	           	if (histTable[i].last >= max){
	           		max = histTable[i].last;
	           		rm_info_idx = i;
	           	}
	       }
    	}
    	else {  // History table has some free slots
    		histTable[ninfo] = histTable[index];
    		rm_info_idx = ninfo;
    		ninfo++;
    	}
	        histTable[rm_info_idx] = histTable[index];
	        histTable[index].hist[0] = timer;
	        for ( int j = 1; j < mgr.numLastRef; ++j )
	        	histTable[index].hist[j] = 0;
	        histTable[index].last = timer;
	        histTable[index].pageNo.pid = mgr.current_pin_page.pid;
      	}
      }
  
  /**
   * This pushes the given frame to the end of the list.
   * @param frameNo	the frame number
   */

  private void update(int frameNo)
  {
     int index;

     for ( index=0; index < nframes; ++index )
        if ( frames[index] == frameNo )
            break;
     update_Hist(index);
	 while ( ++index < nframes )
		 frames[index-1] = frames[index];
	 frames[nframes-1] = frameNo;
  }
  /**
   * Calling super class the same method
   * Initializing the frames[] with number of buffer allocated
   * by buffer manager
   * set number of frame used to zero
   *
   * @param	mgr	a BufMgr object
   * @see	BufMgr
   * @see	Replacer
   */
    public void setBufferManager( BufMgr mgr )
     {
        super.setBufferManager(mgr);
        frames = new int [numBuffers];
        nframes = 0;
        histTable = new HistInfo[Hist_size];
        for (int i=0; i<Hist_size; i++)  // initialize histTable
        	histTable[i] = new HistInfo(mgr.numLastRef);
     }

/* public methods */

  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public LRUK(BufMgr mgrArg)
    {
      super(mgrArg);
      frames = null;
    }
  
  /**
   * call super class the same method
   * pin the page in the given frame number 
   * move the page to the end of list  
   *
   * @param	 frameNo	 the frame number to pin
   * @exception  InvalidFrameNumberException
   */
 public void pin(int frameNo) throws InvalidFrameNumberException
 {
    super.pin(frameNo);

    update(frameNo);
    
 }

  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using LRUK policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */

 public int pick_victim() throws BufferPoolExceededException
 {
   int frame;
   int victim = 0;
   int index;
   boolean victim_found = false;
   
   timer = System.nanoTime()/1000;   // microseconds
   long max_backward_distance = timer;
   
   /**
    * if the buffer is not full (have some free frames), free frame will be selected 
    * as victim and no need to remove current page out of buffer pool and then update 
    * the history reference information of the current pinned page
    */
   
    if ( nframes < numBuffers ) {
        frame = nframes++;
        frames[frame] = frame;
        histTable[frame].last = timer;
        histTable[frame].hist[0] = timer;
        for ( int i = 1; i < mgr.numLastRef; ++i )
        	histTable[frame].hist[i] = 0;
        histTable[frame].pageNo.pid = mgr.current_pin_page.pid;
        state_bit[frame].state = Pinned;
        (mgr.frameTable())[frame].pin();
        return frame;
    }

    
    /**
     * if the buffer is full (have no free frame), the victim is selected based on
     * the maximum value of backward distance 
     */
    for ( int i = 0; i < numBuffers; ++i ) {
         frame = frames[i];
        if ( state_bit[frame].state != Pinned ) {
        	if (((timer - histTable[frame].last) > TIME_OUT_PERIOD) && (histTable[frame].hist[mgr.numLastRef - 1] < max_backward_distance)){
        		victim = frame;
        		max_backward_distance = histTable[frame].hist[mgr.numLastRef - 1];
        		victim_found = true;
        	}
        }
    }
    
    /**
     * if the victim if found, update given frames to the end of the list (@frames)
     * and update the history reference information of the current pinned page
     */
    if (victim_found) {
    	frame = victim;
        state_bit[frame].state = Pinned;
        (mgr.frameTable())[frame].pin();

        for ( index=0; index < nframes; ++index )
            if ( frames[index] == frame )
                break;
	   	 while ( ++index < nframes )
			 frames[index-1] = frames[index];
		 frames[nframes-1] = frame;
        update_Hist(frame);
        return frame;
    }
    else {
        throw new BufferPoolExceededException (null, "BUFMGR: BUFFER_EXCEEDED.");	
    }
 }
 
 
 
 

    
    /**
     * get the name of replacement policy
     *
     * @return	return the name of replacement policy used
     */  
      public String name() { return "LRUK"; }    
   
      /**
       * get the last reference for given @index 
       *
       * @return	return the last reference 
       */  
      public long last(int index) { return histTable[index].last; }  
      
      /**
       * get the state of given @index page 
       *
       * @return	return the state
       */ 
      
      public boolean PageIsPinned(int index) { return (state_bit[index].state == Pinned); }  
      
      /**
       * get pages in the current buffer pool
       *
       * @return	return the  @Frames
       */  
          
      public int[] getFrames() { 
    	  int[] Frames = new int[numBuffers];
    	  int length = numBuffers;
    	  for ( int i = 0; i < numBuffers; ++i ) {
    		  Frames[i] = histTable[i].pageNo.pid;
    	  }
    	  return Frames;
    			  }  

      /**
       * get the history references for given @index 
       *
       * @return	return the history reference 
       */  
      public long HIST(int pageNo, int index) { return (histTable[pageNo].hist[index]/1000); } //miliseconds
          
  /**
   * print out the information of frame usage
   */  
 public void info()
 {
    super.info();

    System.out.print( "LRUK REPLACEMENT");
    
    for (int i = 0; i < nframes; i++) {
        if (i % 5 == 0)
	System.out.println( );
	System.out.print( "\t" + frames[i]);
        
    }
    System.out.println();
 }
  
}



