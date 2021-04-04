import type { Configuration } from 'webpack'

const config: Configuration = {
  entry: './example.ts',
  target: 'node',
  module: {
    rules: [{
      test: /\.ts$/,
      use: ['ts-loader', './webpack-loader.ts']
    }]
  },
  resolve: {
    extensions: ['.ts']
  }
}

export default config
