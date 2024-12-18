// Copyright 2023 The SeamDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::Arc;

use datafusion::common::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue,
    ScalarFunctionArgs,
    ScalarUDFImpl,
    Signature,
    TypeSignature,
    Volatility,
};

use crate::sql::context::SqlContext;

#[derive(Debug)]
pub struct CurrentCatalog {
    context: Arc<SqlContext>,
    signature: Signature,
    aliases: Vec<String>,
}

impl CurrentCatalog {
    pub fn new(context: Arc<SqlContext>) -> Self {
        Self {
            context,
            signature: Signature::new(TypeSignature::Any(0), Volatility::Stable),
            aliases: vec!["current_database".to_string()],
        }
    }
}

impl ScalarUDFImpl for CurrentCatalog {
    fn as_any(&self) -> &(dyn Any + 'static) {
        self
    }

    fn name(&self) -> &str {
        "current_catalog"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(self.context.current_catalog().map(|s| s.to_string()))))
    }
}

#[derive(Debug)]
pub struct CurrentUser {
    context: Arc<SqlContext>,
    signature: Signature,
    aliases: Vec<String>,
}

impl CurrentUser {
    pub fn new(context: Arc<SqlContext>) -> Self {
        Self {
            context,
            signature: Signature::new(TypeSignature::Any(0), Volatility::Stable),
            aliases: vec!["current_role".to_string(), "user".to_string()],
        }
    }
}

impl ScalarUDFImpl for CurrentUser {
    fn as_any(&self) -> &(dyn Any + 'static) {
        self
    }

    fn name(&self) -> &str {
        "current_user"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(self.context.current_user().to_string()))))
    }
}

#[derive(Debug)]
pub struct InetClientPort {
    context: Arc<SqlContext>,
    signature: Signature,
    aliases: Vec<String>,
}

impl InetClientPort {
    pub fn new(context: Arc<SqlContext>) -> Self {
        Self { context, signature: Signature::new(TypeSignature::Any(0), Volatility::Stable), aliases: vec![] }
    }
}

impl ScalarUDFImpl for InetClientPort {
    fn as_any(&self) -> &(dyn Any + 'static) {
        self
    }

    fn name(&self) -> &str {
        "inet_client_port"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(self.context.addr().port().into()))))
    }
}
