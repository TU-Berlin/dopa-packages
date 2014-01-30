package eu.stratosphere.sopremo.base;


import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.io.JsonParseException;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecord;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.*;
import net.internetmemory.commons.searching.AdaptedResponse;
import net.internetmemory.commons.searching.Query;
import net.internetmemory.commons.searching.QueryStringQuery;
import net.internetmemory.commons.searching.ResultResponse;
import net.internetmemory.service.APICredentials;
import net.internetmemory.service.lookup.LookupServiceClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
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

@Name(verb = "imrsearch")
@InputCardinality(0)
public class ImrSearch extends ElementaryOperator<ImrSearch> {

    protected static final String IMR_QUERY_PARAMETER = "ser_IMR_QUERY_PARAMETER";
    protected static final String DM_API_KEY_PARAMETER = "ser_api_key_parameter";
    protected static final String DM_MIN_DATE = "ser_dm_mindate";
    protected static final String DM_MAX_DATE = "ser_dm_maxdate";

    private IJsonNode queryParameterNode = null;
    private String  queryParameterNodeString=null;
    private String dmApiKeyString = null;
    private String mindate = null;
    private String maxdate = null;

	public static class ImrMignifySearch implements InputFormat<SopremoRecord, GenericInputSplit> {

        // REST API root URL
        public static final String REST_API_ROOT_URI = "http://service.mignify.com/";
        // Sample API key
        public static final APICredentials SAMPLE_CREDENTIAL = new APICredentials("4p9sh6qc", "rolh2a4cgn6j1i1");

        private LookupServiceClient client;

        private Query query;

        private AdaptedResponse response;

        private Iterator<ResultResponse> iterator;

		private EvaluationContext context;

        private String queryParameter;

        private IObjectNode resultNode;

        private IObjectNode documentIdNode;

        private TextNode urlTextNode;

        private String apiKey;

        private String minDate;

        private String maxDate;

		@Override
		public void configure(Configuration parameters) {
			this.context = SopremoUtil.getEvaluationContext(parameters);
            SopremoEnvironment.getInstance().setEvaluationContext(context);
			queryParameter = parameters.getString(IMR_QUERY_PARAMETER, null);
			apiKey = parameters.getString(DM_API_KEY_PARAMETER, null);
            minDate = parameters.getString(DM_MIN_DATE, null);
            maxDate = parameters.getString(DM_MAX_DATE, null);
		}

		/*
		 * (non-Javadoc)
		 *
		 * @see
		 * eu.stratosphere.pact.common.io.GenericInputFormat#createInputSplits
		 * (int)
		 */
		@Override
		public GenericInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
            GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                splits[i] = new GenericInputSplit(i);
            }
            return splits;
		}

		/*
		 * (non-Javadoc)
		 *
		 * @see
		 * eu.stratosphere.pact.common.io.GenericInputFormat#getInputSplitType()
		 */
		@Override
		public Class<GenericInputSplit> getInputSplitType() {
			return GenericInputSplit.class;
		}

        @Override
        public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
            return null;
        }

        /*
                 * (non-Javadoc)
                 *
                 * @see
                 * eu.stratosphere.pact.common.io.GenericInputFormat#open(eu.stratosphere
                 * .nephele.template.GenericInputSplit)
                 */
		@Override
		public void open(final GenericInputSplit split) throws IOException {
			if (split.getSplitNumber() == 0) {
                // Create new instance of full test search service client
                client = new LookupServiceClient();

                // Set the root URI of the full text search service
                try {
                    client.setRestApiRootUri(REST_API_ROOT_URI);
                } catch (URISyntaxException e) {
                    throw new IOException(e);
                }

                // Set the credential to use the service
                client.useCredentials(SAMPLE_CREDENTIAL);

                query = new QueryStringQuery(queryParameter);

                response = client.searchContent("mignify", "dopaCleaning", query);

                iterator = response.results.iterator();

                resultNode = new ObjectNode();
                documentIdNode = new ObjectNode();
                IObjectNode poolnode = new ObjectNode();
                poolnode.put("id", new TextNode("imr"));
                poolnode.put("crawl", new TextNode("dopaCleaning"));
                documentIdNode.put("pool", poolnode);
                urlTextNode = new TextNode();

			} else {
                iterator = null;
			}

		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (iterator == null) {
				return true;
			}
			return !this.iterator.hasNext();
		}

        @Override
        public boolean nextRecord(final SopremoRecord record) throws IOException {
            if (this.reachedEnd()) {
                throw new IOException("End of input split is reached");
            }
            ResultResponse resultResponse = iterator.next();
            String url = resultResponse.getProperties().get("url");
            url = reverseDomain(url);
            urlTextNode.setValue(url);
            documentIdNode.put("uri", urlTextNode);
            resultNode.put("documentID", documentIdNode);
            record.setNode(resultNode);
            return true;
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub
        }
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result;
		return result;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
		GenericDataSource<?> contract = new GenericDataSource<ImrMignifySearch>(
				ImrMignifySearch.class, String.format("ImrMignifySearch %s", queryParameterNodeString));

		final PactModule pactModule = new PactModule(0, 1);
        SopremoUtil.setEvaluationContext(contract.getParameters(), context);
        SopremoUtil.setLayout(contract.getParameters(), layout);
        contract.getParameters().setString(IMR_QUERY_PARAMETER, queryParameterNodeString);
		pactModule.getOutput(0).setInput(contract);
		return pactModule;
	}

	@Property(preferred = true)
	@Name(noun = "for")
	public void setqueryParameter(EvaluationExpression value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		queryParameterNode = value.evaluate(NullNode.getInstance());
        queryParameterNodeString = queryParameterNode.toString();
	}

	@Property(preferred = false)
	@Name(noun = "key")
	public void setKeyParameter(EvaluationExpression value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		IJsonNode node = value.evaluate(NullNode.getInstance());
		dmApiKeyString = node.toString();
	}

    @Property(preferred = false)
    @Name(noun = "mindate")
    public void setMinDate(EvaluationExpression value) {
        if (value == null)
            throw new NullPointerException("value expression must not be null");
        IJsonNode node = value.evaluate(NullNode.getInstance());
        mindate = node.toString();
    }

    @Property(preferred = false)
    @Name(noun = "maxdate")
    public void setMaxDate(EvaluationExpression value) {
        if (value == null)
            throw new NullPointerException("value expression must not be null");
        IJsonNode node = value.evaluate(NullNode.getInstance());
        maxdate = node.toString();
    }

    protected static String reverseDomain (String url) {
        Pattern urlSplitter = Pattern.compile("(.*//)([^/:]+)(.*)");
        Matcher matcher = urlSplitter.matcher(url);
        if (matcher.find()) {
            String pre = matcher.group(1);
            String domain = matcher.group(2);
            String post = matcher.group(3);
            StringBuilder builder = new StringBuilder(url.length());
            builder.append(pre);

            int last = domain.length()-1;
            int start = domain.lastIndexOf('.', last);
            while (start != -1) {
                builder.append(domain, start + 1, last + 1);
                builder.append('.');
                last = start - 1;
                start = domain.lastIndexOf('.', last);
            }
            builder.append(domain,start + 1, last+1);
            builder.append(post);
            return builder.toString();
        } else {
            // TODO: deal with failure
        }
        return url;
    }

}
