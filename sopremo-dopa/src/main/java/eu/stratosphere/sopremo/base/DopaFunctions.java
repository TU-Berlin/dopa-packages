package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.function.SopremoFunction1;
import eu.stratosphere.sopremo.function.SopremoFunction2;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.type.*;

/**
 * Created by mleich on 05/02/14.
 */
public class DopaFunctions implements BuiltinProvider {

    @Name(verb = "replaceMissing")
    public static final SopremoFunction REPLACE_MISSING = new SopremoFunction2<IJsonNode, IJsonNode> () {
        @Override
        protected IJsonNode call(IJsonNode input, IJsonNode replacement) {
            if (input == MissingNode.getInstance()) {
                return  replacement;
            } else {
                return input;
            }
        }
    };

    @Name(verb = "exists")
    public static final SopremoFunction EXISTS = new SopremoFunction1<IJsonNode> () {

        @Override
        protected IJsonNode call(IJsonNode input) {
            return BooleanNode.valueOf(input != MissingNode.getInstance());
        }
    };

    @Name(verb = "addToArray")
    public static final SopremoFunction ADDTOARRAY = new SopremoFunction2<IJsonNode, BooleanNode>() {
        @Override
        protected IJsonNode call(IJsonNode input, BooleanNode condition) {
            IArrayNode<IJsonNode> array = new ArrayNode<IJsonNode>();
            if (condition.getBooleanValue()) {
                array.add(input);
            }
            return array;
        }
    };


}