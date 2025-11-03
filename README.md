# Package Template

This is a template package for creating function stream functions for AgentStream. This template provides a complete project structure with all necessary files for developing, testing, and deploying custom packages that process data streams.

## Overview

A package contains one or more functions that process data streams and can be deployed to the AgentStream platform for production use. This template demonstrates how to create a simple function that returns the current time, serving as a starting point for your own custom functions.

## Prerequisites

Before using this template, ensure you have the following development environment:

- **Python Environment**: Python 3.10 or higher
- **Docker Environment**: Docker installed and running
- **AgentStream**: AgentStream platform installed and accessible
- **AgentStream CLI**: AgentStream command-line tool installed

## Quick Start

### 1. Clone the Template

```bash
# Clone from the official template repository
git clone git@github.com/agentstream/package-template

# Or clone your forked repository
git clone git@github.com/<your-github-username>/<your-repo>
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Local Testing

Create a `config.yaml` file for local testing:

```yaml
pulsar:
  serviceUrl: "pulsar://127.0.0.1:6650"
  authPlugin: ""
  authParams: ""

module: "getCurrentTime"

sources:
  - pulsar:
      topic: "get-current-time-source"

requestSource:
  pulsar:
    topic: "get-current-time-request"

subscriptionName: "test-sub"

sink:
  pulsar:
    topic: "get-current-time-sink"

config:
  format: "%Y-%m-%d %H:%M:%S %Z%z"
```

### 4. Run Locally

```bash
# Set configuration path and run
FS_CONFIG_PATH=config.yaml python main.py
```

### 5. Test with AgentStream CLI

```bash
# Send a test request
ascli rpc --topic get-current-time-request --json '{}'
```

## Project Structure

```
package-template/
├── main.py              # Main function implementation
├── package.yaml         # Package definition for AgentStream
├── config.yaml          # Local testing configuration
├── Dockerfile           # Container image definition
├── requirements.txt     # Python dependencies
├── test_main.py         # Unit tests
├── pytest.ini          # Test configuration
├── Makefile            # Build and test commands
└── README.md           # Project documentation
```

## Function Development

### Basic Function Structure

Functions in AgentStream follow a standard pattern:

```python
def get_current_time(context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
    # Access configuration
    time_format = context.get_config("format")
    
    # Process data
    now = datetime.datetime.now()
    
    # Return results
    return {"result": f"The current time is {now.strftime(time_format)}."}
```

### Function Parameters

- **`context`**: Provides access to configuration and runtime information
- **`data`**: Input data dictionary containing the message payload
- **Returns**: A dictionary containing the processed results

### Using Pydantic for Data Validation

It's recommended to use Pydantic for input/output data structure definition:

```python
from pydantic import BaseModel

class User(BaseModel):
    name: str

def process_user(context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
    user = User.model_validate(data)
    return {"greeting": f"Hello, {user.name}!"}
```

## Testing

### Running Tests

#### Option 1: Using Make
```bash
# Run tests
make test

# Run tests with coverage
make test-cov

# Install test dependencies
make install-test-deps
```

#### Option 2: Using pytest directly
```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest test_main.py -v

# Run tests with coverage (requires pytest-cov)
pip install pytest-cov
python -m pytest test_main.py --cov=main --cov-report=term-missing -v
```

### Test Coverage

The test suite covers:

- Default time format behavior
- Custom time format handling
- Empty data dictionary handling
- None data handling
- Various time format patterns (date-only, 12-hour, ISO format)

### Test Structure

- `MockFSContext`: A mock implementation of the FSContext interface
- `TestGetCurrentTime`: Test class containing all test cases
- Each test method follows the Arrange-Act-Assert pattern
- Uses `unittest.mock.patch` to mock datetime for consistent test results

### Adding New Tests

To add new tests:

1. Add new test methods to the `TestGetCurrentTime` class
2. Follow the existing naming convention: `test_<description>`
3. Use descriptive docstrings explaining what each test validates
4. Follow the Arrange-Act-Assert pattern for clear test structure

## Building and Deployment

### Build Docker Image

```bash
# Build for current platform
IMG=<your-user-name>/time-function:latest make build-image

# Build for multiple platforms
IMG=<your-user-name>/time-function:latest PLATFORMS=linux/arm64,linux/amd64 make docker-buildx
```

### Package Definition

Update the `package.yaml` file with your function details:

```yaml
apiVersion: fs.functionstream.github.io/v1alpha1
kind: Package
metadata:
  name: current-time
spec:
  displayName: Get Current Time
  description: "A function for getting the current time."
  functionType:
    cloud:
      image: "your-registry/time-function:latest"  # Update with your image
  modules:
    getCurrentTime:
      displayName: Get Current Time
      description: "A tool that returns the current time."
      sourceSchema: |
        {
          "type": "object",
          "properties": {
            "name": {"type": "string"}
          }
        }
      sinkSchema: "Returns a greeting with current timestamp"
      config:
        format:
          displayName: "Time format"
          description: "The format for displaying the time"
          type: "string"
          required: true
```

### Install to AgentStream

```bash
kubectl apply -f package.yaml
```

## Advanced Topics

### FSModule Implementation

For complex functions with state management, implement the `FSModule` interface:

```python
from function_stream.module import FSModule
from function_stream.context import FSContext

class MyCustomModule(FSModule):
    def init(self, context: FSContext):
        """Initialize the module with configuration and setup"""
        self.context = context
        self.counter = 0
        self.format_string = context.get_config("format")
    
    async def process(self, context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming data asynchronously"""
        self.counter += 1
        result = self.process_data(data)
        return {
            "result": result,
            "counter": self.counter,
            "format_used": self.format_string
        }
```

### FSContext Usage

The `FSContext` provides access to configuration and runtime information:

```python
def process_data(context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
    # Get configuration values
    api_key = context.get_config("api_key")
    timeout = context.get_config("timeout") or 30
    
    # Get all configurations
    all_configs = context.get_configs()
    
    # Get module name
    module_name = context.get_module()
    
    return {"processed": True, "config": all_configs}
```

## Configuration Reference

### Local Testing Configuration

The `config.yaml` file supports the following fields:

- **`pulsar`**: Pulsar broker connection settings
- **`module`**: Name of the Python function to execute
- **`sources`**: List of topics for continuous message consumption
- **`requestSource`**: Topic for request-response pattern messages
- **`subscriptionName`**: Subscription name for the consumer
- **`sink`**: Output topic configuration
- **`config`**: Custom configuration parameters for your function

### Package Configuration

Key fields in `package.yaml`:

- **`metadata.name`**: Unique identifier for your package
- **`spec.displayName`**: Human-readable name shown in the system
- **`spec.functionType.cloud.image`**: Your Docker image containing the function code
- **`spec.modules`**: Defines the functions available in this package
- **`sourceSchema`**: JSON schema or natural language description for input data
- **`sinkSchema`**: JSON schema or natural language description for output data
- **`config`**: User-configurable parameters for the function

## Best Practices

1. **Use Pydantic**: For data validation and clear schema definitions
2. **Error Handling**: Implement proper error handling for production use
3. **Configuration**: Provide sensible defaults and validate required configurations
4. **Testing**: Write comprehensive tests covering edge cases
5. **Documentation**: Include clear descriptions for all configurable items
6. **Logging**: Use appropriate logging levels for debugging and monitoring

## Troubleshooting

### Common Issues

1. **Configuration not found**: Ensure `FS_CONFIG_PATH` is set correctly
2. **Module not found**: Check that the module name in `config.yaml` matches your function
3. **Dependencies missing**: Install all requirements with `pip install -r requirements.txt`
4. **Docker build failures**: Ensure Docker is running and you have proper permissions

### Debug Mode

For detailed debugging, the function will print logs showing:
- Consumer creation for topics
- Function service startup
- Module initialization
