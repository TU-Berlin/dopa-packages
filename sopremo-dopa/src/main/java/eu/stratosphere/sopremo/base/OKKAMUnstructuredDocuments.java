package eu.stratosphere.sopremo.base;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
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
import org.okkam.dopa.apis.beans.request.GetUnstructuredDocumentsQuery;
import org.okkam.dopa.apis.client.OkkamDopaIndexClient;
import org.okkam.dopa.apis.client.OkkamIndexClientParameters;
import org.okkam.dopa.apis.client.compression.CompressionAlgorithm;
import org.okkam.dopa.apis.response.GetOkkamAnnotatedEntitiesResponse;
import org.okkam.dopa.apis.response.GetUnstructuredDocumentsResponse;
import org.okkam.dopa.buffer.beans.Detection;
import org.okkam.dopa.buffer.beans.Document;
import org.okkam.dopa.buffer.beans.DopaDatapools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: mleich
 * Date: 7/11/2013
 * Time: 14:13
 * To change this template use File | Settings | File Templates.
 */
@Name(verb = "getOkkamUnstructuredDocuments")
@InputCardinality(1)
public class OKKAMUnstructuredDocuments extends ElementaryOperator<OKKAMUnstructuredDocuments> {

    public static final String ACCESS_PARAMETER = "OKKAM.document.access.parameter";
    public static final String DATAPOOL_PARAMETER = "OKKAM.document.datapool.parameter";
    public static final String CRAWLID_PARAMETER = "OKKAM.document.crawlid.parameter";
    public static final String LIMIT_PARAMETER = "OKKAM.document.limit.parameter";

    private String crawlID = null;
    private String dataPool = null;
    private int limit = 10;

    private EvaluationExpression accessExtression;

    public static class Implementation extends SopremoMap {

        private OkkamDopaIndexClient client;

        private EvaluationExpression accessExpression;

        private String crawl;

        private String poolid;

        private int limit;

        @Override
        public void open(Configuration parameters) {
            super.open(parameters);
            OkkamIndexClientParameters okkamparameters = new OkkamIndexClientParameters("okkam4.disi.unitn.it:80", "/okkam-index");
            okkamparameters.setCompressor(CompressionAlgorithm.LZ4);
            client = new OkkamDopaIndexClient(okkamparameters);
            accessExpression = SopremoUtil.getObject(parameters, ACCESS_PARAMETER, null);
            poolid = parameters.getString(DATAPOOL_PARAMETER, null);
            crawl = parameters.getString(CRAWLID_PARAMETER, null);
            limit = parameters.getInteger(LIMIT_PARAMETER, 10);
        }

        @Override
        protected void map(IJsonNode value, JsonCollector<IJsonNode> out) {
            String entityID =  accessExpression.evaluate(value).toString();

            GetUnstructuredDocumentsQuery query = new GetUnstructuredDocumentsQuery();
            query.setQueryOkkamIds("okkamized-content-entity:" + entityID);
            query.setCrawlid(crawl);
            query.setDatapool(DopaDatapools.valueOf(poolid));
            query.setLimit(limit);
            try {
                GetUnstructuredDocumentsResponse response = client.getUnstructuredDocuments(query, false);
                List<String> urls =  response.getOkkamIdUris();
                ObjectNode poolresult = new ObjectNode();
                poolresult.put("id", new TextNode(poolid));
                poolresult.put("crawl", new TextNode(crawl));
                ObjectNode docID = new ObjectNode();
                docID.put("pool", poolresult);
                ObjectNode result = new ObjectNode();
                result.put("entityID", new TextNode(entityID));
                result.put("documentID", docID);
                for (String url : urls) {
                    docID.put("uri", new TextNode(url));
                    out.collect(result);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void configureOperator(Operator contract, Configuration stubConfiguration) {
        super.configureOperator(contract, stubConfiguration);
        SopremoUtil.setObject(stubConfiguration, ACCESS_PARAMETER, accessExtression);
        stubConfiguration.setString(DATAPOOL_PARAMETER, dataPool);
        stubConfiguration.setString(CRAWLID_PARAMETER, crawlID);
        stubConfiguration.setInteger(LIMIT_PARAMETER, limit);
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

    @Property(preferred = true)
    @Name(noun = "crawlID")
    public void setCrawlID(EvaluationExpression value) {
        if (value == null)
            throw new NullPointerException("DocumentID access expression must not be null");
        crawlID = value.evaluate(NullNode.getInstance()).toString();
    }

    @Property(preferred = true)
    @Name(noun = "dataPool")
    public void setDataPool(EvaluationExpression value) {
        if (value == null)
            throw new NullPointerException("DocumentID access expression must not be null");
        dataPool = value.evaluate(NullNode.getInstance()).toString();
    }

    @Property(preferred = true)
    @Name(noun = "limit")
    public void setLimit(EvaluationExpression value) {
        if (value == null)
            throw new NullPointerException("Limit expression must not be null");
        IJsonNode limitnode = value.evaluate(NullNode.getInstance());
        if (limitnode instanceof INumericNode) {
            limit = ((INumericNode) limitnode).getIntValue();
        } else {
            throw new ClassCastException("Limit must be numeric");
        }
    }
}
