package eu.stratosphere.sopremo.base;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.*;
import net.internetmemory.commons.searching.AdaptedResponse;
import net.internetmemory.commons.searching.Query;
import net.internetmemory.service.APICredentials;
import net.internetmemory.service.content.ContentServiceClient;
import net.internetmemory.service.content.api.Feature;
import net.internetmemory.service.content.api.MetaInformation;

import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: mleich
 * Date: 7/11/2013
 * Time: 14:13
 * To change this template use File | Settings | File Templates.
 */
@Name(verb = "imrGetContent")
@InputCardinality(1)
public class ImrGetDocumentContent extends ElementaryOperator<ImrGetDocumentContent> {

    public static final String CRAWLID_PARAMETER = "IMR.getContent.crawlID.parameter";

    /** default values for the crawlid parameter. This will cause the crawlID to be read from the passed JSON objects */
    private static final String TUPPLECRAWLID = "<IMR.getContent.USETUPLECRAWLID>";

    private String crawlId = TUPPLECRAWLID;

    public static class Implementation extends SopremoMap {

        private String crawlIdParameter;

        // REST API root URL
        public static final String REST_API_ROOT_URI = "http://service.mignify.com/";
        // Sample API key
        public static final APICredentials SAMPLE_CREDENTIAL = new APICredentials("4p9sh6qc", "rolh2a4cgn6j1i1");

        Set<String> features;

        private ContentServiceClient client;

        private Query query;

        private AdaptedResponse response;

        private TextNode contentNode;



        @Override
        public void open(Configuration parameters) {
            super.open(parameters);

            crawlIdParameter = parameters.getString(CRAWLID_PARAMETER, TUPPLECRAWLID);

            /* Create a content service client for "worldnews" collection in "mignify" cluster*/
            client = new ContentServiceClient();
		    /* Set REST API root URI */
            try {
                client.setRestApiRootUri(REST_API_ROOT_URI);
            } catch (URISyntaxException e) {
                e.printStackTrace(System.err);
            }
		    /* Use credential */
            client.useCredentials(SAMPLE_CREDENTIAL);

            contentNode = new TextNode();

            features = new HashSet<String>();
            features.add("baseline:curatedForNer");
        }

        @Override
        protected void map(IJsonNode value, JsonCollector<IJsonNode> out) {
            if (value instanceof IObjectNode) {
                IObjectNode objValue = (IObjectNode) value;

                IJsonNode documentID = objValue.get("documentID");
                if (documentID instanceof IObjectNode) {
                    IJsonNode pool = ((IObjectNode) documentID).get("pool");
                    if (pool instanceof IObjectNode) {
                        IObjectNode objPool = (IObjectNode) pool;
                        if (objPool.get("id").toString().equals("imr")) {
                            final String tuppleCrawlId = objPool.get("crawl").toString();
                            String uri = ((IObjectNode) documentID).get("uri").toString();
                            // convert to url for IMR access
                            uri = ImrSearch.reverseDomain(uri);
                            // compute the crawlid
                            String crawlid = null;
                            if (crawlIdParameter.equals(TUPPLECRAWLID)) {
                                // take the crawlid from the current json value
                                crawlid = tuppleCrawlId;
                            } else {
                                // use that passed parameter for all of them, instead
                                crawlid = crawlIdParameter;
                            }
                            // TODO currently we fetch the most recent version, but we should use the timestamp from the OKKAM index
                            MetaInformation version = client.getNearestResource("mignify", crawlid, uri, System.currentTimeMillis());
                            if (version != null) {
                                List<Feature> result = client.getFeatures("mignify", crawlid, uri, version.getTimestamp(), features);
                                String curatedForNer = result.get(0).getValue().toString();

                                contentNode.setValue(curatedForNer);

                                objValue.put("content", contentNode);
                            } else {
                                System.out.println("#####- GETCONTENT Could not find version for: " + uri);
                            }
                        }
                    }

                }
            }
            out.collect(value);
        }
    }

    @Override
    protected void configureOperator(Operator contract, Configuration stubConfiguration) {
        super.configureOperator(contract, stubConfiguration);
        stubConfiguration.setString(CRAWLID_PARAMETER, crawlId);
    }

    @Property(preferred = true)
    @Name(noun = "crawlID")
    public void setCrawlId(EvaluationExpression value) {
        if (value == null)
            throw new NullPointerException("crawlID expression must not be null");
        this.crawlId = value.evaluate(NullNode.getInstance()).toString();
    }
}
