package eu.stratosphere.sopremo.base;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.*;
import org.okkam.dopa.apis.beans.request.GetOkkamAnnotatedEntitiesQuery;
import org.okkam.dopa.apis.client.OkkamDopaIndexClient;
import org.okkam.dopa.apis.response.GetOkkamAnnotatedEntitiesResponse;
import org.okkam.dopa.buffer.beans.Detection;
import org.okkam.dopa.buffer.beans.Document;
import org.okkam.dopa.buffer.beans.DopaDatapools;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: mleich
 * Date: 7/11/2013
 * Time: 14:13
 * To change this template use File | Settings | File Templates.
 */
@Name(verb = "getOkkamEntityAnnotations")
@InputCardinality(1)
public class OKKAMGetEntities extends ElementaryOperator<OKKAMGetEntities> {

    public static final String ACCESS_PARAMETER = "OKKAM.entity.access.parameter";

    private String crawlID = null;
    private String dataPool = null;

    private EvaluationExpression accessExtression;

    public static class Implementation extends SopremoMap {

        private OkkamDopaIndexClient client;

        private EvaluationExpression accessExpression;

        @Override
        public void open(Configuration parameters) {
            super.open(parameters);
            client = new OkkamDopaIndexClient("okkam4.disi.unitn.it:80", "okkam-index", 10);
            accessExpression = SopremoUtil.getObject(parameters, ACCESS_PARAMETER, null);
        }

        @Override
        protected void map(IJsonNode value, JsonCollector<IJsonNode> out) {
            value = accessExpression.evaluate(value);
            if (value instanceof IObjectNode) {
                IJsonNode pool = ((IObjectNode) value).get("pool");
                if (pool instanceof IObjectNode) {
                    IJsonNode id = ((IObjectNode) pool).get("id");
                    IJsonNode crawlid = ((IObjectNode) pool).get("crawl");
                    String poolid = id.toString();
                    String crawl = crawlid.toString();
                    GetOkkamAnnotatedEntitiesQuery query = new GetOkkamAnnotatedEntitiesQuery();
                    query.setCrawlid(crawl);
                    query.setDatapool(DopaDatapools.valueOf(poolid));
                    IJsonNode urls = ((IObjectNode) value).get("url");
                    List<String> queryurls  = null;
                    if (urls instanceof IArrayNode) {
                        IArrayNode urlarray = (IArrayNode) urls;
                        queryurls = new ArrayList<String>(urlarray.size());
                        for (int i = 0; i < urlarray.size(); i++) {
                            IJsonNode node = urlarray.get(i);
                            queryurls.add(node.toString());
                        }
                    } else {
                        queryurls = new ArrayList<String>(1);
                        queryurls.add(urls.toString());
                    }
                    query.setUrls(queryurls);
                    try {
                        GetOkkamAnnotatedEntitiesResponse response = client.getOkkamAnnotatedEntities(query, false);
                        List<Document> docs =  response.getOkkamAnnotations();
                        ObjectNode poolresult = new ObjectNode();
                        poolresult.put("id", new TextNode(poolid));
                        poolresult.put("crawl", new TextNode(crawl));
                        ObjectNode result = new ObjectNode();
                        result.put("pool", poolresult);
                        for (Document doc : docs) {
                            System.out.println("##### Found " + docs.size() + "documents");
                            result.put("url", new TextNode(doc.getUrl()));
                            for (Detection detection : doc.getDetections()) {
                                System.out.println("##### Found " + doc.getDetections().size() + "detections");
                                if (detection.getOkkamId() != null ) {
                                    result.put("entityID", new TextNode(detection.getOkkamId()));
                                    out.collect(result);
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    protected void configureContract(Contract contract, Configuration stubConfiguration, EvaluationContext context, SopremoRecordLayout layout) {
        super.configureContract(contract, stubConfiguration, context, layout);
        SopremoUtil.setObject(stubConfiguration, ACCESS_PARAMETER, accessExtression);
    }

    @Property(preferred = true)
    @Name(noun = "for")
    public void setDocumentField(EvaluationExpression value) {
        if (value == null)
            throw new NullPointerException("DocumentID access expression must not be null");
        this.accessExtression = value.clone();
        this.accessExtression = accessExtression.replace(new InputSelection(0), EvaluationExpression.VALUE);
        System.out.println("set access expression " + accessExtression.toString());
    }
}
