package eu.stratosphere.sopremo.base;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.io.GenericInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Schema;
import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.io.GeneratorInputFormat;
import eu.stratosphere.sopremo.io.GeneratorInputSplit;
import eu.stratosphere.sopremo.io.JsonParseException;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.io.SopremoFormat;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecord;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

@Name(verb = "datamarket")
public class DataMarketFormat extends SopremoFormat {

	protected static final String DM_URL_PARAMETER = "ser_dm_url_parameter";
	protected static final String DM_API_KEY_PARAMETER = "ser_api_key_parameter";

	private String  urlParameter=null;
	private String apiKey = null;

//	public static class DataMarketInputFormat extends GenericInputFormat {
	public static class DataMarketInputFormat extends GeneratorInputFormat {

		private EvaluationContext context;

		private String urlParameter;
		
		private String apiKey;

		private Iterator<IJsonNode> nodeIterator;

		@Override
		public void configure(Configuration parameters) {
			this.context = (EvaluationContext) SopremoUtil.getEvaluationContext(parameters);
			SopremoEnvironment.getInstance().setEvaluationContext(context);
			SopremoUtil.configureWithTransferredState(this, InputFormat.class, parameters);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.pact.common.io.GenericInputFormat#createInputSplits
		 * (int)
		 */
		@Override
		public GeneratorInputSplit[] createInputSplits(final int minNumSplits)
				throws IOException {

			return super.createInputSplits(minNumSplits);
		}

		/**
		 * Advanced input Split generation, requires to register
		 * DataMarketInputSplit with Nepeheles kryo instances
		 * 
		 * @param minNumSplits
		 * @return
		 */
		private DataMarketInputSplit[] createDistributedSplits(
				final int minNumSplits) {
			// fetch data from DataMarket, convert to our record format, and
			// store records in the requested input splits
			// this assumes that DM does not return too much data

			String response = fetchDataMarketResponse(this.urlParameter, apiKey);
			
			// convert record format
			ArrayNode<IJsonNode> records = convertDataMarketResponse(response);

			// create input splits from the gathered objects
			final int numInputSplits = Math.min(minNumSplits, records.size());
			final DataMarketInputSplit[] inputSplits = new DataMarketInputSplit[numInputSplits];

			// compute number of records per split
			int recordsPerSplit = records.size() / numInputSplits
					+ Math.min(1, records.size() % numInputSplits);

			// create split objects
			Iterator<IJsonNode> iterator = records.iterator();
			for (int i = 0; i < numInputSplits; i++) {
				// number of records in the current split
				int numrecords = (i < numInputSplits - 1) ? recordsPerSplit
						: (records.size() - i * recordsPerSplit)
								% recordsPerSplit;
				IJsonNode[] nodes = new IJsonNode[numrecords];
				for (int j = 0; j < numrecords; j++) {
					nodes[j] = iterator.next();
				}
				DataMarketInputSplit split = new DataMarketInputSplit(i, nodes);
				inputSplits[i] = split;
			}

			return inputSplits;
		}

		private String fetchDataMarketResponse(String urlParameter, String apiKey) {
			// fetch data from URL
			BufferedReader reader = null;
			String result = "";
			try {
				String urlstring = "http://datamarket.com/api/v1/series.json?ds=" + urlParameter;
				if (apiKey != null) {
					urlstring += "&secret_key=" + apiKey;
				}
				URL url = new URL(urlstring);
				reader = new BufferedReader(new InputStreamReader(
						url.openStream()));
				StringBuffer buffer = new StringBuffer();
				int read;
				char[] chars = new char[1024];
				while ((read = reader.read(chars)) != -1) {
					buffer.append(chars, 0, read);
				}
				result = buffer.toString();
//				System.out.println("Answer from DM:");
//				System.out.println(result);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.pact.common.io.GenericInputFormat#getInputSplitType()
		 */
	/*	@Override
		public Class<GeneratorInputSplit> getInputSplitType() {
			//return GenericInputSplit.class;
			return getInputSplitType();
		}
*/
		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.pact.common.io.GenericInputFormat#open(eu.stratosphere
		 * .nephele.template.GenericInputSplit)
		 */
		@Override
		public void open(final GenericInputSplit split) throws IOException {
			super.open(split);
			
			if (split.getSplitNumber() == 0) {
				String response = fetchDataMarketResponse(this.urlParameter, apiKey);
				// convert record format
				ArrayNode<IJsonNode> records = convertDataMarketResponse(response);
				
				this.nodeIterator = records.iterator();
			} else {
				this.nodeIterator = null;
			}
			
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (nodeIterator == null) {
				return true;
			}
			return !this.nodeIterator.hasNext();
		}

		@Override
		public boolean nextRecord(final SopremoRecord record) throws IOException {
			if (this.reachedEnd())
				throw new IOException("End of input split is reached");

			final IJsonNode value = this.nodeIterator.next();
			record.setNode(value);
			return true;
		}

		protected static ArrayNode<IJsonNode> convertDataMarketResponse(
				String jsonString) {
			// remove Data Market API call around content
			Pattern pattern = Pattern.compile(
					"jsonDataMarketApi\\(\\[(.+)\\]\\)", Pattern.DOTALL);
			Matcher matcher = pattern.matcher(jsonString);
			if (!matcher.find()) {
				return null;
			}
			String jsoncontent = matcher.group(1);
			JsonParser parser = new JsonParser(jsoncontent);
//			System.out.println("JSONCONTENT");
//			System.out.println(jsoncontent);
			// send Json content to parser
			IObjectNode obj;
			try {
				obj = (IObjectNode) parser.readValueAsTree();
			} catch (JsonParseException e) {
				return null;
			}

			// getting the columns of file at first
			// to read data following the given hierarchy in Data-market files
			String[] dimensions;
			@SuppressWarnings("unchecked")
			IArrayNode<IObjectNode> columns = (IArrayNode<IObjectNode>) obj
					.get("columns");
			dimensions = new String[columns.size()];
			for (int i = 0; i < columns.size(); i++) {
				IObjectNode subcolumn = columns.get(i);
				IJsonNode column = subcolumn.get("title");
				String titel = column.toString();
				dimensions[i] = titel;
			}

			// array for the converted data items
			ArrayNode<IJsonNode> finalJsonArr = new ArrayNode<IJsonNode>();

			// fill target array with the values from the "data" array
			@SuppressWarnings("unchecked")
			IArrayNode<IArrayNode<IJsonNode>> data = (IArrayNode<IArrayNode<IJsonNode>>) obj
					.get("data");
			for (int j = 0; j < data.size(); j++) {
				IArrayNode<IJsonNode> subDataArray = data.get(j);

				IObjectNode extractedObj = new ObjectNode();
				for (int n = 0; n < subDataArray.size(); n++) {
					if (dimensions[n].equals("Date")) {
						extractedObj.put(dimensions[n],
								handleDateForDatamarket(subDataArray.get(n)));
					} else {
						extractedObj.put(dimensions[n], subDataArray.get(n));
					}
				}
				finalJsonArr.add(extractedObj);
			}
			return finalJsonArr;
		}

		private static TextNode handleDateForDatamarket(IJsonNode timeIn) {
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date date;
			SimpleDateFormat f = new SimpleDateFormat("dd-MMM-yyyy");
			;
			String time = "";
			try {
				date = (Date) formatter.parse(timeIn.toString());
				time = f.format(date).intern();

			} catch (ParseException e1) {
				System.out.println("Exception :" + e1);
				System.out
						.println("Exception : Given Text does not fit our Date-Format.");
			}
			return new TextNode(time);
		}
	}

	/**
	 * CURRENTLY UNUSED! This type needs to be registered with Nephele...
	 * registration order is important, so this needs to be done during
	 * initialization of nephele in a reproducible order!
	 * 
	 * Input split for DataMarketAccessInputFormat. In addition to the number
	 * the split also contains the actual data, that each split should return.
	 * This way, the dm web API is only queried once and we still can distribute
	 * the result across the required number of nodes.
	 * 
	 * @author mleich
	 */
	public static class DataMarketInputSplit extends GenericInputSplit {

		private IJsonNode[] records;

		public DataMarketInputSplit() {
			super();
		}

		public DataMarketInputSplit(int number, IJsonNode[] records) {
			super(number);
			this.records = records;
		}

		public IJsonNode[] getRecords() {
			return records;
		}
	}

	
	
	

   protected static class DatasetParser {

	private Map<String,IArrayNode<IJsonNode>> dimensions = new HashMap<String,IArrayNode<IJsonNode>> ();
	
     public String parseDS(String dscontent) {
	
		String finalDS="";
		
		// parse content from json file, multiple ds is allowed	
		JsonParser parser = new JsonParser(dscontent);
		IArrayNode<IObjectNode>datasets = null;
	    try {
	    	datasets =(IArrayNode<IObjectNode>) parser.readValueAsTree();
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  	     //call each dataset like: 12rb!e4s=7a:e4t=5	 or 17tm!kqc=p
	    for (int i = 0; i < datasets.size(); i++) {
	    	 
	    	String di="";
	    	
	    	IJsonNode ds_id=datasets.get(i).get("ds_id");
	    	String one_ds_id=ds_id.toString();
	        if (!(datasets.get(i).get("dimension") instanceof MissingNode)) {
	        	IObjectNode dimensions=(IObjectNode) datasets.get(i).get ("dimension");
	        	Iterator<Entry<String, IJsonNode>> iterator=dimensions.iterator();
		        while(iterator.hasNext()){
		        	Entry<String, IJsonNode> e=iterator.next();  
		           	TextNode eachDim=(TextNode)checkDimension(ds_id,e.getKey(),e.getValue());
				   	 
		  	    	di+=eachDim+":";	    	//   System.out.println(di);   //e4s=7a:e4t=5:
		   	    	one_ds_id+="!"+di.substring(0, di.lastIndexOf(":"));  		      
		       	}
	        }
	        	 
	       	finalDS+=one_ds_id+"/";
     }  
	  // set up the whole (multi-)dataset together
	    finalDS=finalDS.substring(0, finalDS.lastIndexOf("/"));
	     
		return finalDS;	
	}	
	/*
	 * Transfer each dimension under its ds_id
	 * return a form for datamarket api 
	 * {e4s: 7a}   ==> e4s=7a
	 */
    public TextNode checkDimension(IJsonNode ds, String id, IJsonNode value) {
    	 IArrayNode<IJsonNode> dimArray = dimensions.get (ds.toString());
    	 TextNode  tmp = null;
    	 if (dimArray == null) {
    		 // fetch dimensions from DataMarket          	      		
      		    BufferedReader reader = null;
      		    String dsData="";
      		    try {     		       
      	            URL url = new URL("http://datamarket.com/api/v1/info.json?ds="+ds.toString());
      		        reader = new BufferedReader(new InputStreamReader(url.openStream()));   		        
      		        StringBuffer buffer = new StringBuffer();	      
      		        int read;
      		        char[] chars = new char[1024];
      		       
      		        while ((read = reader.read(chars)) != -1)
      		          buffer.append(chars, 0, read); 
      		          dsData=buffer.toString();
         
      		    } catch (Exception e) {
      				// TODO Auto-generated catch block
      				e.printStackTrace();
      			}
      	
    			// remove Data Market API call around content
    			Pattern pattern = Pattern.compile("jsonDataMarketApi\\((.+)\\)", Pattern.DOTALL);
    			Matcher matcher = pattern.matcher(dsData);
    			if (!matcher.find()) {
    				return null;
    			}
    			String jsoncontent = matcher.group(1);
    			JsonParser parser = new JsonParser(jsoncontent);
         		
    	 		IArrayNode<IObjectNode> dims = null;   	
      		try {
      			dims =(IArrayNode<IObjectNode>) parser.readValueAsTree();
      	    	
      		} catch (JsonParseException e) {
      			// TODO Auto-generated catch block
      			e.printStackTrace();
      		}	        	      		
      		//call each dimension values	  
      	    //values|subValues::  [{id: 6a, title: Eastern Europe}, {id: 3e, iso3166: PT, title: Portugal},.....
      		//subValues.get(0)::{id: 6a, title: Eastern Europe}
      	    	dimArray =(IArrayNode<IJsonNode>) dims.get(0).get("dimensions");
    		    this.dimensions.put(ds.toString(), dimArray); 
    	 }
    		
     		for (int i = 0; i < dimArray.size(); i++) {
     	    	IJsonNode d_id_key=((IObjectNode) dimArray.get(i)).get("id");
     	    	IJsonNode d_id_value=((IObjectNode) dimArray.get(i)).get("title");
     	    	IJsonNode values=((IObjectNode) dimArray.get(i)).get("values");
     	    	IArrayNode<IJsonNode> valueArray=(IArrayNode<IJsonNode>)values;
     	    	
     	    	if(id.equals(d_id_value.toString())){
     	    		id= d_id_key.toString();
     	    	}   	    	
     	    	 for (int j = 0; j < valueArray.size(); j++) {
     	    		 IJsonNode keyValue=((IObjectNode) valueArray.get(j)).get("id"); 
     	    		 IJsonNode valueValue=((IObjectNode) valueArray.get(j)).get("title"); 
     	    		
     	    		 if(value.equals(valueValue)){
     	    			value=keyValue;     	    	
     	    		 }     	    		 
     	    	//set one dimension	     	         	         	  
    	        tmp=new TextNode(id+"="+value.toString());
     	    	 }
     	   }     		     	 
     		return tmp;	       	
     	}	
}
	
	
	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result;
		return result;
	}

	@Property(preferred = true)
	@Name(noun = "for")
	public void setURLParameter(String urlParameterNode) {
		if (urlParameterNode == null)
			throw new NullPointerException("value expression must not be null");
		
		DatasetParser ds = new DatasetParser();
		urlParameter=ds.parseDS(urlParameterNode);
		
//		System.out.println("set urlParameter expression "
//				+ urlParameterNode.toString());
	}
	
	@Property(preferred = false)
	@Name(noun = "key")
	public void setKeyParameter(String value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		apiKey = value;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.io.SopremoFileFormat#getInputFormat()
	 */
	@Override
	public Class<? extends SopremoInputFormat<?>> getInputFormat() {
		return (Class<? extends SopremoInputFormat<?>>) DataMarketInputFormat.class;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.io.SopremoFileFormat#getPreferredFilenameExtension()
	 */
	/*@Override
	protected String getPreferredFilenameExtension() {
		return null;
	}*/
	
}
