package eu.stratosphere.sopremo.base;

import org.junit.Assert;
import org.junit.Test;
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
 * Created by mleich on 31/01/14.
 */
public class TestOKKAMClient {

    @Test
    public void testGetEntityAnnotations () {

        GetOkkamAnnotatedEntitiesQuery query = new GetOkkamAnnotatedEntitiesQuery();
        query.setCrawlid("test2");
        query.setDatapool(DopaDatapools.valueOf("imr"));

        List<String> uris = new ArrayList<String>(1);
        uris.add("http://com.aol.realestate/blog/about-partners/");
        //uris.add("blab badkfsjsljf");
        query.setUris(uris);

        OkkamIndexClientParameters parameters = new OkkamIndexClientParameters("okkam4.disi.unitn.it:80", "/okkam-index");
        parameters.setCompressor(CompressionAlgorithm.LZ4);
        OkkamDopaIndexClient client = new OkkamDopaIndexClient(parameters);

        GetOkkamAnnotatedEntitiesResponse response = null;
        try {
            response = client.getOkkamAnnotatedEntities(query, false);
            List<Document> docs =  response.getOkkamAnnotations();
            Assert.assertNotNull(docs);
            Assert.assertTrue(docs.size()> 0);
            for (Document doc : docs) {
                List<Detection> detections = doc.getDetections();
                Assert.assertNotNull(detections);
                Assert.assertTrue(detections.size() > 0);
                for (Detection det : detections) {
                    System.out.println(det.getOkkamId());
                }
            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetDocumentIDs () {

        OkkamIndexClientParameters parameters = new OkkamIndexClientParameters("okkam4.disi.unitn.it:80", "/okkam-index");
        parameters.setCompressor(CompressionAlgorithm.LZ4);
        OkkamDopaIndexClient client = new OkkamDopaIndexClient(parameters);

        GetUnstructuredDocumentsQuery query = new GetUnstructuredDocumentsQuery();
        query.setQueryOkkamIds("okkamized-content-entity:ens:eid-a7ecb0a0-ab70-4c74-bc7f-bc65ecfa8c58");
        query.setCrawlid("test2");
        query.setDatapool(DopaDatapools.valueOf("imr"));
        query.setLimit(2);
        try {
            GetUnstructuredDocumentsResponse response = client.getUnstructuredDocuments(query, false);
            List<String> urls =  response.getOkkamIdUris();
            Assert.assertNotNull(urls);
            Assert.assertTrue(urls.size() > 0);
            Assert.assertTrue(urls.size() <= 2);
            for (String url : urls) {
                System.out.println(url);
            }
        }
        catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
}